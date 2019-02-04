require 'scraperwiki'
require 'mechanize'

url_base = "https://www.burnside.sa.gov.au/Planning-Business/Planning-Development/Development-Applications/Development-Applications-on-Public-Notification"

agent = Mechanize.new
OpenSSL::SSL::VERIFY_PEER = OpenSSL::SSL::VERIFY_NONE
agent.verify_mode = OpenSSL::SSL::VERIFY_NONE
page = agent.get(url_base)

page.search('div.list-container a').each do |a|
  info_url = a["href"]
  page = agent.get(info_url)

  record = {
    "council_reference" => page.search('span.field-label:contains("Application number") + span.field-value').inner_text.strip.to_s,
    "address" => page.search('span.field-label:contains("Address") ~ span').inner_text.gsub(/\u00a0/, ' ').gsub('View Map', '').strip + ", SA",
    "description" => page.search('span.field-label:contains("Nature of development") + span.field-value').inner_text.strip.to_s,
    "info_url" => info_url,
    "comment_url" => "mailto:burnside@burnside.sa.gov.au",
    "date_scraped" => Date.today.to_s,
    "on_notice_to" => Date.parse(page.search('h2.side-box-title:contains("Closing Date") + div').inner_text.strip.split(', ')[0]).to_s,
  }

  if (ScraperWiki.select("* from data where `council_reference`='#{record['council_reference']}'").empty? rescue true)
    puts "Saving record " + record['council_reference'] + " - " + record['address']
#     puts record
    ScraperWiki.save_sqlite(['council_reference'], record)
  else
    puts "Skipping already saved record " + record['council_reference']
  end
end
