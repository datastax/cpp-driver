include Nanoc::Helpers::Breadcrumbs
include Nanoc::Helpers::Rendering
include Nanoc::Helpers::HTMLEscape

def child_of?(item, parent)
  if item.parent == parent
    true
  elsif item.parent.nil?
    false
  else
    child_of?(item.parent, parent)
  end
end

def highlight_code(code)
  markup = ::Rouge.highlight(code, 'c++', 'html')
  markup.sub!(/<pre><code class="highlight">/,'<pre class="highlight"><code class="cpp">')
  markup.sub!(/<\/code><\/pre>/,"</code></pre>")
  markup.strip!
  markup
end

def section_title(section)
  case section
  when :user_defined            then 'User Defined'
  when :public_type             then 'Types'
  when :public_func             then 'Functions'
  when :public_attrib           then 'Attributes'
  when :public_slot             then 'Slots'
  when :signal                  then 'Signals'
  when :dcop_func               then ''
  when :property                then 'Properties'
  when :event                   then 'Events'
  when :public_static_func      then 'Static Functions'
  when :public_static_attrib    then 'Static Attributres'
  when :protected_type          then 'Protected Types'
  when :protected_func          then 'Protected Functions'
  when :protected_attrib        then 'Protected Attributes'
  when :protected_slot          then 'Protected Slots'
  when :protected_static_func   then 'Protected Static Functions'
  when :protected_static_attrib then 'Protected Static Attributes'
  when :package_type            then 'Package Types'
  when :package_func            then 'Package Functions'
  when :package_attrib          then 'Package Attributes'
  when :package_static_func     then 'Package Static Functions'
  when :package_static_attrib   then 'Package Static Attributes'
  when :private_type            then 'Private Types'
  when :private_func            then 'Private Functions'
  when :private_attrib          then 'Private Attributes'
  when :private_slot            then 'Private Slots'
  when :private_static_func     then 'Private Static Functions'
  when :private_static_attrib   then 'Private Static Attributes'
  when :friend                  then 'Friends'
  when :related                 then 'Related'
  when :define                  then 'Macros'
  when :prototype               then 'Prototypes'
  when :typedef                 then 'Types'
  when :enum                    then 'Enums'
  when :func                    then 'Functions'
  when :var                     then 'Variables'
  end
end

def tag_title(tag)
  case tag
  when :see       then 'See also'
  when :return    then 'Return'
  when :author    then 'Author'
  when :authors   then 'Authors'
  when :version   then 'Version'
  when :since     then 'Since'
  when :date      then 'Date'
  when :note      then 'Note'
  when :warning   then 'Warning'
  when :pre       then 'Pre'
  when :post      then 'Post'
  when :copyright then 'Copyright'
  when :invariant then 'Invariant'
  when :remark    then 'Remark'
  when :attention then 'Attention'
  when :par       then 'Par'
  when :rcs       then ''
  end
end

def format_params(function)
  function.params.map do |p|
    if p.type
      "#{p.type} #{p.name}"
    else
      p.name
    end
  end.join(", ")
end

def lunr_index
  unless File.directory?('node_modules')
    npm_bin  = Cliver.detect!('npm', '~> 2.1')

    system(npm_bin, 'install')
  end

  node_bin = Cliver.detect!('node', '~> 0.10')

  items = []

  @items.each do |item|
    next unless ['xml', 'md'].include?(item[:extension])

    items << {
      'title' => item[:title],
      'text'  => item[:text],
      'path'  => item.path
    }
  end

  json = JSON.dump(items)

  IO.popen("#{node_bin} nanoc/create-index.js", "r+") do |node|
    node.puts json
    node.close_write
    node.gets
  end
end

def pages_json
  data = {}

  @items.each do |item|
    next unless ['xml', 'md'].include?(item[:extension])

    data[item.path] = {
      'title'   => item[:title],
      'summary' => item[:summary],
      'path'    => item.path
    }
  end

  JSON.dump(data)
end
