# encoding: utf-8

module Docs
  class AddIDsToHeadersFilter < Nanoc::Filter
    identifiers :add_ids_to_headers

    def run(content, arguments={})
      doc = Nokogiri::HTML.fragment(content)
      doc.css("h1, h2, h3, h4, h5, h6").each do |header|
        header['id'] ||= header.content.downcase.gsub(',', '').gsub(/\W+/, '-').gsub(/^-|-$/, '')

        anchor = Nokogiri::HTML.fragment(%Q(<a class="anchor" href="##{header['id']}" aria-hidden="true"><span class="glyphicon glyphicon-link"></span></a>))

        header.add_child(anchor)
      end
      doc.to_s.gsub("<li>\n<a href=", '<li><a href=')
    end
  end
end
