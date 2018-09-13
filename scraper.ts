// Parses the development application at the South Australian City of Burnside web site and places
// them in a database.
//
// This is partly based on the scraper at https://github.com/LoveMyData/burnside.
//
// Michael Bone
// 8th July 2018

"use strict";

import * as cheerio from "cheerio";
import * as request from "request-promise-native";
import * as sqlite3 from "sqlite3";
import * as urlparser from "url";
import * as moment from "moment";
import * as pdfjs from "pdfjs-dist";

sqlite3.verbose();

const DevelopmentApplicationsUrl = "https://www.burnside.sa.gov.au/Planning-Business/Planning-Development/New-Planning-Applications-Register";
const CommentUrl = "mailto:burnside@burnside.sa.gov.au";

declare const global: any;
declare const process: any;

// Sets up an sqlite database.

async function initializeDatabase() {
    return new Promise((resolve, reject) => {
        let database = new sqlite3.Database("data.sqlite");
        database.serialize(() => {
            database.run("create table if not exists [data] ([date_scraped] text, [description] text, [info_url] text, [council_reference] text primary key, [address] text, [on_notice_to] text, [comment_url] text)");
            resolve(database);
        });
    });
}

// Inserts a row in the database if it does not already exist.

async function insertRow(database, developmentApplication) {
    return new Promise((resolve, reject) => {
        let sqlStatement = database.prepare("insert or ignore into [data] values (?, ?, ?, ?, ?, ?, ?)");
        sqlStatement.run([
            developmentApplication.scrapeDate,
            developmentApplication.description,
            developmentApplication.informationUrl,
            developmentApplication.applicationNumber,
            developmentApplication.address,
            null,
            developmentApplication.commentUrl
        ], function(error, row) {
            if (error) {
                console.log(error);
                reject(error);
            }
            else {
                if (this.changes > 0)
                    console.log(`    Inserted: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\" and description \"${developmentApplication.description}\" into the database.`);
                else
                    console.log(`    Skipped: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\" and description \"${developmentApplication.description}\" because it was already present in the database.`);
                sqlStatement.finalize();  // releases any locks
                resolve(row);
            }
        });
    });
}

// A bounding rectangle.

interface Rectangle {
    x: number,
    y: number,
    width: number,
    height: number
}

// An element (consisting of text and a bounding rectangle) in a PDF document.

interface Element extends Rectangle {
    text: string
}

// Gets the highest Y co-ordinate of all elements that are considered to be in the same row as
// the specified element.

function getRowTop(elements: Element[], startElement: Element) {
    let top = Number.MAX_VALUE;
    for (let element of elements)
        if (element.y < startElement.y + startElement.height && element.y + element.height > startElement.y)
            if (element.y < top)
                top = element.y;
    return top;
}

// Constructs a rectangle based on the intersection of the two specified rectangles.

function constructIntersection(rectangle1: Rectangle, rectangle2: Rectangle): Rectangle {
    let x1 = Math.max(rectangle1.x, rectangle2.x);
    let y1 = Math.max(rectangle1.y, rectangle2.y);
    let x2 = Math.min(rectangle1.x + rectangle1.width, rectangle2.x + rectangle2.width);
    let y2 = Math.min(rectangle1.y + rectangle1.height, rectangle2.y + rectangle2.height);
    if (x2 >= x1 && y2 >= y1)
        return { x: x1, y: y1, width: x2 - x1, height: y2 - y1 };
    else
        return { x: 0, y: 0, width: 0, height: 0 };
}

// Gets the text in the rectangle, where the rectangle is delineated by the positions in which
// the three specified strings of text are found.

function getBoundedText(elements: Element[], topLeftText: string, rightText: string, bottomText: string) {
    // Construct a bounding rectangle in which the expected text should appear.  Any elements
    // over 50% within the bounding rectangle will be assumed to be part of the expected text.

    let topLeftElement = elements.find(element => element.text.startsWith(topLeftText));
    let rightElement = (rightText === undefined) ? undefined : elements.find(element => element.text.startsWith(rightText));
    let bottomElement = (bottomText === undefined) ? undefined: elements.find(element => element.text.startsWith(bottomText));
    if (topLeftElement === undefined)
        return undefined;

    let x = topLeftElement.x + topLeftElement.width;
    let y = topLeftElement.y;
    let width = (rightElement === undefined) ? Number.MAX_VALUE : (rightElement.x - x);
    let height = (bottomElement === undefined) ? Number.MAX_VALUE : (bottomElement.y - y);

    let bounds: Rectangle = { x: x, y: y, width: width, height: height };

    // Gather together all elements that are at least 50% within the bounding rectangle.

    let intersectingElements: Element[] = []
    for (let element of elements) {
        let intersectingBounds = constructIntersection(element, bounds);
        let intersectingArea = intersectingBounds.width * intersectingBounds.height;
        let elementArea = element.width * element.height;
        if (elementArea > 0 && intersectingArea * 2 > elementArea && element.text !== ":")
            intersectingElements.push(element);
    }

    // Sort the elements by Y co-ordinate and then by X co-ordinate.

    let elementComparer = (a, b) => (a.y > b.y) ? 1 : ((a.y < b.y) ? -1 : ((a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0)));
    intersectingElements.sort(elementComparer);

    // Join the elements into a single string.

    return intersectingElements.map(element => element.text).join(" ").trim().replace(/\s\s+/g, " ");
}

// Parses the details from the elements associated with a single development application.

function parseApplicationElements(elements: Element[], informationUrl: string) {
    let applicationNumber = getBoundedText(elements, "DA Number", "Applicant's Name", "Description");
    let receivedDate = getBoundedText(elements, "Application Date", undefined, "Applicant's Name");
    if (receivedDate === undefined)
        receivedDate = getBoundedText(elements, "Registered", undefined, "Applicant's Name");  // some PDFs use the text "Registered" and others use "Application Date"
    let description = getBoundedText(elements, "Description", "Applicant's Name", "Property Address");
    let address = getBoundedText(elements, "Property Address", "Applicant's Name", "Legal Description");

    // A valid application must at least have an application number and an address.

    if (applicationNumber === "" || address === "")
        return undefined;

    let parsedReceivedDate = moment(receivedDate, "D/MM/YYYY", true);  // allows the leading zero of the day to be omitted
    return {
        applicationNumber: applicationNumber,
        address: address,
        description: ((description === "") ? "No description provided" : description),
        informationUrl: informationUrl,
        commentUrl: CommentUrl,
        scrapeDate: moment().format("YYYY-MM-DD"),
        receivedDate: parsedReceivedDate.isValid() ? parsedReceivedDate.format("YYYY-MM-DD") : ""
    }
}

// Reads and parses development application details from the specified PDF.

async function parsePdf(url: string) {
    let developmentApplications = [];

    // Read the PDF.

    let buffer = await request({ url: url, encoding: null, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);

    // Parse the PDF.  Each page has details of a single application (which in some cases may
    // overflow onto subsequent pages).

    const pdf = await pdfjs.getDocument({ data: buffer });

    for (let index = 0; index < pdf.numPages; index++) {
        let page = await pdf.getPage(index + 1);

        // Construct a text element for each item from the parsed PDF information.

        let textContent = await page.getTextContent();
        let viewport = await page.getViewport(1.0);
        let elements: Element[] = textContent.items.map(item => {
            let transform = pdfjs.Util.transform(viewport.transform, item.transform, [ 1, 0, 0, -1, 0, 0 ]);
            
            // Work around the issue https://github.com/mozilla/pdf.js/issues/8276 (heights are
            // exaggerated).  The problem seems to be that the height value is too large in some
            // PDFs.  Provide an alternative, more accurate height value by using a calculation
            // based on the transform matrix.

            let workaroundHeight = Math.sqrt(transform[2] * transform[2] + transform[3] * transform[3]);
            return { text: item.str, x: transform[4], y: transform[5], width: item.width, height: workaroundHeight };
        })

        // Group the elements into sections based on where the "DA Number" text starts (and
        // any other element the "DA Number" element lines up with horizontally).

        let startElements = elements.filter(element => element.text.startsWith("DA Number"));
        let yComparer = (a, b) => (a.y > b.y) ? 1 : ((a.y < b.y) ? -1 : 0);
        startElements.sort(yComparer);

        let applicationElementGroups: Element[][] = [];
        for (let index = 0; index < startElements.length; index++) {
            // Determine the highest Y co-ordinate of this row and the next row.

            let rowTop = getRowTop(elements, startElements[index]);
            let nextRowTop = (index + 1 < startElements.length) ? getRowTop(elements, startElements[index + 1]) : Number.MAX_VALUE;

            // Extract all elements between the two rows.

            applicationElementGroups.push(elements.filter(element => element.y >= rowTop && element.y + element.height < nextRowTop));
        }

        // Parse the development application from each section.

        for (let applicationElements of applicationElementGroups) {
            let developmentApplication = parseApplicationElements(applicationElements, url);
            if (developmentApplication !== undefined)
                developmentApplications.push(developmentApplication);
        }
    }

    return developmentApplications;
}

// Gets a random integer in the specified range: [minimum, maximum).

function getRandom(minimum: number, maximum: number) {
    return Math.floor(Math.random() * (Math.floor(maximum) - Math.ceil(minimum))) + Math.ceil(minimum);
}

// Pauses for the specified number of milliseconds.

function sleep(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

// Parses the page at the specified URL.

async function main() {
    // Ensure that the database exists.

    let database = await initializeDatabase();

    // Retrieve the main page.

    console.log(`Retrieving page: ${DevelopmentApplicationsUrl}`);
    let body = await request({ url: DevelopmentApplicationsUrl, proxy: process.env.MORPH_PROXY });
    let $ = cheerio.load(body);
    await sleep(2000 + getRandom(0, 5) * 1000);

    let pdfUrls: string[] = [];
    for (let element of $("a.ext-pdf[href$='.pdf']").get()) {
        let pdfUrl = new urlparser.URL(element.attribs.href, DevelopmentApplicationsUrl).href;
        if (!pdfUrls.some(url => url === pdfUrl))  // avoid duplicates
            pdfUrls.push(pdfUrl);
    }

    if (pdfUrls.length === 0) {
        console.log("No PDF URLs were found on the page.");
        return;
    }

    // Select the most recent PDF.  And randomly select one other PDF (avoid processing all PDFs
    // at once because this may use too much memory, resulting in morph.io terminating the current
    // process).

    let selectedPdfUrls: string[] = [];
    selectedPdfUrls.push(pdfUrls.shift());
    if (pdfUrls.length > 0)
        selectedPdfUrls.push(pdfUrls[getRandom(1, pdfUrls.length)]);
    if (getRandom(0, 2) === 0)
        selectedPdfUrls.reverse();

    // Parse the most recent PDF file and randomly select one other PDF file.

    for (let pdfUrl of selectedPdfUrls) {
        console.log(`Parsing document: ${pdfUrl}`);
        let developmentApplications = await parsePdf(pdfUrl);
        console.log(`Parsed ${developmentApplications.length} development application(s) from document: ${pdfUrl}`);

        // Attempt to avoid reaching 512 MB memory usage (this will otherwise result in the
        // current process being terminated by morph.io).

        if (global.gc)
            global.gc();

        for (let developmentApplication of developmentApplications)
            await insertRow(database, developmentApplication);
    }
}

main().then(() => console.log("Complete.")).catch(error => console.error(error));
