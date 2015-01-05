# encoding: utf-8

module Docs
  class AddTOCFilter < Nanoc::Filter
    identifier :add_toc

    def run(content, params={})
      content.gsub('{{TOC}}') do
        # Find all top-level sections
        doc = Nokogiri::HTML(content)
        headers = doc.css('h2', 'h3').map do |header|
          { :title => header.text, :id => header['id'] }
        end

        if headers.empty?
          next ''
        end

        # Build table of contents
        headers.each_with_object('') do |header, output|
          output << %[<li><a href="##{header[:id]}">#{header[:title]}</a></li>]
        end
      end
    end

  end
end