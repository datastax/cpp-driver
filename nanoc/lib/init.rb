require 'nanoc/toolbox'
require 'redcarpet'
require 'rouge'
require 'nokogiri'
require 'compass'

class Markdown < Redcarpet::Render::SmartyHTML
  def initialize(*args)
    super
  end

  def block_code(code, language)
    case language
    when nil, ''
      "<pre><code>#{CGI.escapeHTML(code)}</code></pre>"
    when 'ditaa'
      data = Base64.encode64(Ditaa.render(code, :separation => false))
      "<img src=\"data:image/png;base64,#{data}\" alt=\"Text Diagram\" class=\"img-rounded img-thumbnail center-block ditaa\" />"
    else
      markup = ::Rouge.highlight(code, language, 'html')
      markup.sub!(/<pre><code class="highlight">/,'<pre class="highlight"><code class="' + language + '">')
      markup.sub!(/<\/code><\/pre>/,"</code></pre>")
      markup.strip!
      markup
    end
  rescue
    "<pre><code>#{CGI.escapeHTML(code)}</code></pre>"
  end
end
