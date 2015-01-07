module Docs
  class APIDataSource < Nanoc::DataSource
    identifier :api

    def up
      super
      @markdown = Redcarpet::Markdown.new(::Markdown.new, {})
    end

    def down
      @markdown = nil
      super
    end

    def prefix
      'api'
    end

    def path_to_xml_dir
      File.expand_path(config.fetch(:doxygen))
    end

    def items
      items    = []
      registry = Doxygen.load(path_to_xml_dir, @markdown)

      registry.each_object do |object|
        next if object.kind == :dir

        items << Nanoc::Item.new('_', {
          :doxygen   => object,
          :title     => object.name,
          :extension => 'xml',
          :text      => Nokogiri::XML(object.raw_content).text,
          :summary   => "#{object.name} <small class=\"text-muted\">#{object.kind}</small>",
          :mtime     => File.mtime(object.file)
        }, prefix + '/' + object.id)
      end

      items << Nanoc::Item.new('_', {
        :doxygen   => registry.index,
        :title     => 'API docs',
        :extension => 'xml',
        :text      => Nokogiri::XML(registry.index.raw_content).text,
        :summary   => "API docs <small class=\"text-muted\">index</small>",
        :mtime     => File.mtime(registry.index_file)
      }, prefix)

      items
    end
  end
end


class Doxygen
  private_class_method :new

  def self.load(path_to_xml_dir, markdown)
    instance = new(markdown)
    instance.load(path_to_xml_dir)
    instance
  end

  attr_reader :index

  def initialize(markdown)
    @markdown = markdown
    @objects  = ::Hash.new
    @index    = Index.new
  end

  def load(path_to_xml_dir)
    index_path   = path_to_xml_dir + '/index.xml'
    objects_path = path_to_xml_dir + '/*.xml'
    Dir.glob(objects_path) do |path|
      next if path == index_path

      raw_xml = File.read(path)
      xml     = Nokogiri::XML.parse(raw_xml)

      xml.xpath('//doxygen/compounddef').each do |element|
        object = load_compound(element)
        object.raw_content  = raw_xml
        @objects[object.id] = object
      end
    end

    @index.file = index_path
    raw_xml = File.read(index_path)
    xml     = Nokogiri::XML.parse(raw_xml)
    @index.raw_content = raw_xml

    xml.xpath('//doxygenindex/compound').each do |element|
      @index.add(load_compound_reference(element))
    end

    self
  end

  def each_object(&block)
    @objects.each_value(&block)
  end

  def index_file
    @index.file
  end

  private

  def load_compound_reference(node)
    Reference.new({
      :id      => node.attr('refid'),
      :name    => load_text(node.at_xpath('name')),
      :kind    => load_compound_kind(node.attr('kind')),
      :members => node.xpath('member').map {|el| load_member_reference(el)}
    })
  end

  def load_member_reference(node)
    MemberReference.new({
      :id    => node.attr('refid'),
      :name  => load_text(node.at_xpath('name')),
      :kind  => load_member_kind(node.attr('kind'))
    })
  end

  def load_compound(node)
    sections   = {}
    attributes = {
      :id         => node.attr('id'),
      :name       => load_text(node.at_xpath('compoundname')),
      :kind       => load_compound_kind(node.attr('kind')),
      :title      => load_text(node.at_xpath('title')),
      :visibility => load_visibility(node.attr('prot')),
      :final      => load_bool(node.attr('final')),
      :sealed     => load_bool(node.attr('sealed')),
      :abstract   => load_bool(node.attr('abstract')),
      :parents    => node.xpath('basecompoundref').map {|el| load_compound_ref(el)},
      :children   => node.xpath('derivedcompoundref').map {|el| load_compound_ref(el)},
      :includes   => node.xpath('includes').map {|el| load_include(el)},
      :includedby => node.xpath('includedby').map {|el| load_include(el)},
      :dirs       => node.xpath('innerdir').map {|el| load_ref(el)},
      :files      => node.xpath('innerfile').map {|el| load_ref(el)},
      :classes    => node.xpath('innerclass').map {|el| load_ref(el)},
      :namespaces => node.xpath('innernamespace').map {|el| load_ref(el)},
      :pages      => node.xpath('innerpage').map {|el| load_ref(el)},
      :groups     => node.xpath('innergroup').map {|el| load_ref(el)},
      :details    => load_docblock(node),
      :sections   => sections,
    }
    attributes.merge!(load_location_info(node.at_xpath('location')))

    node.xpath('sectiondef').each do |el|
      sections[load_section_kind(el.attr('kind'))] = el.xpath('memberdef').map {|e| load_member(e)}
    end

    Compound.new(attributes)
  end

  def load_member(node)
    attributes = {
      :id          => node.attr('id').split('_').last,
      :kind        => load_member_kind(node.attr('kind')),
      :type        => load_markdown_line(node.at_xpath('type')),
      :initializer => load_markdown_line(node.at_xpath('initializer')),
      :exceptions  => load_markdown_line(node.at_xpath('exceptions')),
      :definition  => load_text(node.at_xpath('definition')),
      :argsstring  => load_text(node.at_xpath('argsstring')),
      :name        => load_text(node.at_xpath('name')),
      :params      => node.xpath('param').map {|el| load_param(el)},
      :values      => node.xpath('enumvalue').map {|el| load_enumvalue(el)},
      :visibility  => load_visibility(node.attr('prot')),
      :static      => load_bool(node.attr('static')),
      :const       => load_bool(node.attr('const')),
      :explicit    => load_bool(node.attr('explicit')),
      :inline      => load_bool(node.attr('inline')),
      :virt        => load_bool(node.attr('virt')),
      :volatile    => load_bool(node.attr('volatile')),
      :mutable     => load_bool(node.attr('mutable')),
      :readable    => load_bool(node.attr('readable')),
      :writable    => load_bool(node.attr('writable')),
      :initonly    => load_bool(node.attr('initonly')),
      :settable    => load_bool(node.attr('settable')),
      :gettable    => load_bool(node.attr('gettable')),
      :final       => load_bool(node.attr('final')),
      :sealed      => load_bool(node.attr('sealed')),
      :new         => load_bool(node.attr('new')),
      :add         => load_bool(node.attr('add')),
      :remove      => load_bool(node.attr('remove')),
      :raise       => load_bool(node.attr('raise')),
      :details     => load_docblock(node)
    }

    attributes.merge!(load_location_info(node.at_xpath('location')))

    Member.new(attributes)
  end

  def load_enumvalue(node)
    Value.new({
      :id          => node.attr('id').split('_').last,
      :name        => load_text(node.at_xpath('name')),
      :initializer => load_markdown_line(node.at_xpath('initializer')),
      :details     => load_docblock(node)
    })
  end

  def load_param(node)
    name   = node.at_xpath('declname')
    name ||= node.at_xpath('defname')

    Param.new({
      :name => load_text(name),
      :type => load_markdown_line(node.at_xpath('type'))
    })
  end

  def load_member_kind(value)
    case value
    when 'define'    then :define
    when 'property'  then :property
    when 'event'     then :event
    when 'variable'  then :variable
    when 'typedef'   then :typedef
    when 'enum'      then :enum
    when 'enumvalue' then :enumvalue
    when 'function'  then :function
    when 'signal'    then :signal
    when 'prototype' then :prototype
    when 'friend'    then :friend
    when 'dcop'      then :dcop
    when 'slot'      then :slot
    when 'interface' then :interface
    when 'service'   then :service
    else
      raise ::ArgumentError, "unsupported member kind: #{value.inspect}"
    end
  end

  def load_section_kind(value)
    case value
    when 'user-defined'            then :user_defined
    when 'public-type'             then :public_type
    when 'public-func'             then :public_func
    when 'public-attrib'           then :public_attrib
    when 'public-slot'             then :public_slot
    when 'signal'                  then :signal
    when 'dcop-func'               then :dcop_func
    when 'property'                then :property
    when 'event'                   then :event
    when 'public-static-func'      then :public_static_func
    when 'public-static-attrib'    then :public_static_attrib
    when 'protected-type'          then :protected_type
    when 'protected-func'          then :protected_func
    when 'protected-attrib'        then :protected_attrib
    when 'protected-slot'          then :protected_slot
    when 'protected-static-func'   then :protected_static_func
    when 'protected-static-attrib' then :protected_static_attrib
    when 'package-type'            then :package_type
    when 'package-func'            then :package_func
    when 'package-attrib'          then :package_attrib
    when 'package-static-func'     then :package_static_func
    when 'package-static-attrib'   then :package_static_attrib
    when 'private-type'            then :private_type
    when 'private-func'            then :private_func
    when 'private-attrib'          then :private_attrib
    when 'private-slot'            then :private_slot
    when 'private-static-func'     then :private_static_func
    when 'private-static-attrib'   then :private_static_attrib
    when 'friend'                  then :friend
    when 'related'                 then :related
    when 'define'                  then :define
    when 'prototype'               then :prototype
    when 'typedef'                 then :typedef
    when 'enum'                    then :enum
    when 'func'                    then :func
    when 'var'                     then :var
    else
      raise ::ArgumentError, "unspected section kind: #{value.inspect}"
    end
  end

  def load_location_info(node)
    location = {}
    return location unless node

    location[:file]   = node.attr('file')
    location[:line]   = node.attr('line')   || 1
    location[:column] = node.attr('column') || 1

    location
  end

  def load_docblock(node)
    title          = nil
    description    = load_markdown(node.at_xpath('briefdescription')) + load_markdown(node.at_xpath('detaileddescription'))
    tags           = ::Hash.new
    params         = ::Hash.new
    retvals        = []
    exception      = []
    templateparams = []

    el = node.at_xpath('detaileddescription')
    el && el.traverse do |el|
      case el.name
      when 'simplesect'
        tag = load_simplesect(el)
        (tags[tag.kind] ||= []) << tag
      when 'title'
        title = load_markdown(el)
      when 'parameterlist'
        kind = el.attr('kind')

        case kind
        when 'param'
          el.xpath('parameteritem').each do |child|
            param = load_parameteritem(child)
            params[param.name] = param
          end
        when 'retval'
          el.xpath('parameteritem').each do |child|
            retvals << load_parameteritem(child)
          end
        when 'exception'
          el.xpath('parameteritem').each do |child|
            exceptions << load_parameteritem(child)
          end
        when 'templateparam'
          el.xpath('parameteritem').each do |child|
            templateparams << load_parameteritem(child)
          end
        else
          raise ::ArgumentError, "unsupported parameter list kind: #{kind.inspect}"
        end
      when 'variablelist'
        # TODO: figure how to support these
        raise "unsupported node type: #{el.name}"
      end
    end

    Docblock.new({
      :title          => title,
      :description    => description,
      :tags           => tags,
      :params         => params,
      :retvals        => retvals,
      :exception      => exception,
      :templateparams => templateparams
    })
  end

  def load_parameteritem(node)
    attributes = load_parameteritem_name_and_direction(node.at_xpath('parameternamelist'))
    attributes[:description] = load_markdown(node.at_xpath('parameterdescription'))

    Parameter.new(attributes)
  end

  def load_parameteritem_name_and_direction(node)
    attributes = {}

    child = node.at_xpath('parametername')
    return attributes unless child

    attributes[:name]      = load_text(child)
    attributes[:direction] = load_direction(child)

    attributes
  end

  def load_direction(node)
    value = node && node.attr('direction')
    return :in unless value

    case value
    when 'in'  then :in
    when 'out' then :out
    else
      raise ::ArgumentError, "unsupported direction #{value.inspect}"
    end
  end

  def children_to_markdown(node)
    node.children.inject('') do |text, child|
      text << node_to_markdown(child)
    end
  end

  def node_to_markdown(node)
    case node.name
    when 'text'
      text = node.text
      text.gsub!(/\u00A0/, "&nbsp;")
      text.gsub!(/\A\n+/, '')
      text.gsub!(/\n+\z/, '')
      # text.tr!("\n\t", ' ')
      text.squeeze!(' ')
      text.gsub!(/[\*\_]/, '*' => '\*', '_' => '\_')
      text
    when 'bold'
      '**' + node.text + '**'
    when 'emphasis'
      '*' + node.text + '*'
    when 'computeroutput'
      '<samp>' + node.text + '</samp>'
    when 'preformatted'
      '`` ' + node.text + ' ``'
    when 'orderedlist'
      index = 0
      node.xpath('listitem').inject("\n") do |text, el|
        text << "#{index += 1}. #{node_to_markdown(el)}\n"
      end
    when 'itemizedlist'
      node.xpath('listitem').inject("\n") do |text, el|
        text << "* #{node_to_markdown(el)}\n"
      end
    when 'blockquote'
      node.xpath('para').inject("\n") do |text, el|
        text << "> #{node_to_markdown(el)}\n"
      end
    when 'iexcl', 'cent', 'pound', 'curren', 'yen', 'brvbar', 'sect',
         'copy', 'ordf', 'laquo', 'not', 'shy', 'macr', 'deg', 'plusmn',
         'sup1', 'sup2', 'sup3', 'acute', 'micro', 'middot', 'cedil',
         'ordm', 'raquo', 'frac14', 'frac12', 'frac34', 'iquest',
         'Agrave', 'Aacute', 'Acirc', 'Atilde', 'Aring', 'AElig',
         'Ccedil', 'Egrave', 'Eacute', 'Ecirc', 'Igrave', 'Iacute',
         'Icirc', 'ETH', 'Ntilde', 'Ograve', 'Oacute', 'Ocirc', 'Otilde',
         'times', 'Oslash', 'Ugrave', 'Uacute', 'Ucirc', 'Yacute',
         'THORN', 'szlig', 'agrave', 'aacute', 'acirc', 'atilde', 'aring',
         'aelig', 'ccedil', 'egrave', 'eacute', 'ecirc', 'igrave',
         'iacute', 'icirc', 'eth', 'ntilde', 'ograve', 'oacute', 'ocirc',
         'otilde', 'divide', 'oslash', 'ugrave', 'uacute', 'ucirc',
         'yacute', 'thorn', 'fnof', 'Alpha', 'Beta', 'Gamma', 'Delta',
         'Epsilon', 'Zeta', 'Eta', 'Theta', 'Iota', 'Kappa', 'Lambda',
         'Mu', 'Nu', 'Xi', 'Omicron', 'Pi', 'Rho', 'Sigma', 'Tau',
         'Upsilon', 'Phi', 'Chi', 'Psi', 'Omega', 'alpha', 'beta',
         'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota',
         'kappa', 'lambda', 'mu', 'nu', 'xi', 'omicron', 'pi', 'rho',
         'sigmaf', 'sigma', 'tau', 'upsilon', 'phi', 'chi', 'psi',
         'omega', 'thetasym', 'upsih', 'piv', 'bull', 'hellip', 'prime',
         'Prime', 'oline', 'frasl', 'weierp', 'image', 'euro' 'real',
         'alefsym', 'larr', 'uarr', 'rarr', 'darr', 'harr', 'crarr',
         'lArr', 'uArr', 'rArr', 'dArr', 'hArr', 'forall', 'part',
         'exist', 'empty', 'nabla', 'isin', 'notin', 'ni', 'prod', 'sum',
         'minus', 'lowast', 'radic', 'prop', 'infin', 'ang', 'and', 'or',
         'cap', 'cup', 'int', 'there4', 'sim', 'cong', 'asymp', 'ne',
         'equiv', 'le', 'ge', 'sub', 'sup', 'nsub', 'sube', 'supe',
         'oplus', 'otimes', 'perp', 'sdot', 'lceil', 'rceil', 'lfloor',
         'rfloor', 'lang', 'rang', 'loz', 'spades', 'clubs', 'hearts',
         'diams', 'OElig', 'oelig', 'Scaron', 'scaron', 'circ', 'tilde',
         'ensp', 'emsp', 'thinsp', 'zwnj', 'zwj', 'lrm', 'rlm', 'ndash',
         'mdash', 'lsquo', 'rsquo', 'sbquo', 'ldquo', 'rdquo', 'bdquo',
         'dagger', 'Dagger', 'permil', 'lsaquo', 'rsaquo'
      '&' + node.name + ';'
    when 'ulink'             then "[#{children_to_markdown(node)}](#{node.attr('url')})"
    when 'anchor'            then "[#{children_to_markdown(node)}](##{node.attr('id')})"
    when 'ref'
      kind  = node.attr('kindref')
      refid = node.attr('refid')
      text  = node.text

      return '' unless refid

      if kind == 'member'
        *parts, id = refid.split('_')
        "[`#{text}`](/api/#{parts.join('_')}##{id})"
      else
        "[`#{text}`](/api/#{refid})"
      end
    when 'nonbreakablespace' then '&nbsp;'
    when 'umlaut'            then '&uml;'
    when 'registered'        then '&reg;'
    when 'Aumlaut'           then '&Auml;'
    when 'Eumlaut'           then '&Euml;'
    when 'Iumlaut'           then '&Iuml;'
    when 'Oumlaut'           then '&Ouml;'
    when 'Uumlaut'           then '&Uuml;'
    when 'aumlaut'           then '&auml;'
    when 'eumlaut'           then '&euml;'
    when 'iumlaut'           then '&iuml;'
    when 'oumlaut'           then '&ouml;'
    when 'uumlaut'           then '&uuml;'
    when 'yumlaut'           then '&yuml;'
    when 'Yumlaut'           then '&Yuml;'
    when 'trademark'         then '&trade;'
    when 'linebreak'         then "\n"
    when 'hruler'            then "\n\n---"
    when 'formula', 'ref', 'programlisting', 'indexentry', 'simplesect',
      'parameterlist', 'variablelist'
      # TODO: figure out if we need to support these
      ''
    when 'table', 'heading', 'image', 'dotfile', 'mscfile', 'diafile',
         'toclist', 'language', 'xrefsect', 'copydoc', 'parblock'
      # TODO: figure out how to support these
      raise "unsupported node type: #{node.name}"
    when 'para'
      children_to_markdown(node) + "\n\n"
    else
      children_to_markdown(node)
    end
  end

  def load_markdown(node)
    return unless node

    markdown = node_to_markdown(node)
    markdown.gsub!(/\n{3,}/, "\n\n")
    markdown.strip!
    return '' if markdown.empty?
    @markdown.render(markdown).strip
  end

  def load_markdown_line(node)
    html = load_markdown(node)
    return unless html
    html.sub!('<p>', '')
    html.chomp!('</p>')
    html
  end

  def load_simplesect(node)
    Tag.new({
      :title       => load_text(node.at_xpath('title')),
      :kind        => load_simplesect_kind(node.attr('kind')),
      :description => load_markdown(node.at_xpath('para'))
    })
  end

  def load_simplesect_kind(value)
    case value
    when 'see'       then :see
    when 'return'    then :return
    when 'author'    then :author
    when 'authors'   then :authors
    when 'version'   then :version
    when 'since'     then :since
    when 'date'      then :date
    when 'note'      then :note
    when 'warning'   then :warning
    when 'pre'       then :pre
    when 'post'      then :post
    when 'copyright' then :copyright
    when 'invariant' then :invariant
    when 'remark'    then :remark
    when 'attention' then :attention
    when 'par'       then :par
    when 'rcs'       then :rcs
    else
      raise ::ArgumentError, "invalid tag kind: #{value.inspect}"
    end
  end

  def load_include(node)
    Include.new({
      :id    => node.attr('refid'),
      :file  => node.text,
      :local => load_bool(node.attr('local'))
    })
  end

  def load_compound_kind(value)
    case value
    when 'class'     then :class
    when 'struct'    then :struct
    when 'union'     then :union
    when 'interface' then :interface
    when 'protocol'  then :protocol
    when 'category'  then :category
    when 'exception' then :exception
    when 'service'   then :service
    when 'singleton' then :singleton
    when 'module'    then :module
    when 'type'      then :type
    when 'file'      then :file
    when 'namespace' then :namespace
    when 'group'     then :group
    when 'page'      then :page
    when 'example'   then :example
    when 'dir'       then :dir
    else
      raise ::ArgumentError, "unsupported compound kind: #{kind.inspect}"
    end
  end

  def load_ref(node)
    Ref.new({
      :id         => node.attr('refid'),
      :visibility => load_visibility(node.attr('prot')),
      :tooltip    => node.attr('tooltip'),
      :name       => node.text
    })
  end

  def load_compound_ref(node)
    CompoundRef.new({
      :id         => node.attr('refid'),
      :visibility => load_visibility(node.attr('prot')),
      :virtuality => load_virtuality(node.attr('virt'))
    })
  end

  def load_text(el)
    el && el.text
  end

  def load_visibility(value)
    return :public unless value

    case value
    when 'public'    then :public
    when 'protected' then :protected
    when 'private'   then :private
    when 'package'   then :package
    else
      raise ::ArgumentError, "unsupported visibility: #{value.inspect}"
    end
  end

  def load_virtuality(value)
    return :none unless value

    case value
    when 'non-virtual'  then :none
    when 'virtual'      then :virtual
    when 'pure-virtual' then :pure
    else
      raise ::ArgumentError, "unsupported virtuality: #{value.inspect}"
    end
  end

  def load_bool(value)
    value == 'yes'
  end

  class Object
    def initialize(attributes)
      attributes.each do |(name, value)|
        instance_variable_set(:"@#{name}", value)
      end
    end

    def has?(attribute)
      instance_variable_defined(:"@#{attribute}")
    end
  end

  class Index
    attr_accessor :file, :raw_content
    attr_reader :sections

    def initialize
      @sections = ::Hash.new
    end

    def each_object(*kinds, &block)
      kinds.each do |kind|
        next unless @sections.include?(kind)
        @sections[kind].each(&block)
      end

      self
    end

    def add(reference)
      @sections[reference.kind] ||= []
      @sections[reference.kind] << reference

      self
    end
  end

  class Reference < Object
    attr_reader :id
    attr_reader :name
    attr_reader :kind
    attr_reader :members
  end

  class MemberReference < Object
    attr_reader :id
    attr_reader :name
    attr_reader :kind
  end


  class CodeObject < Object
    attr_reader :id
    attr_reader :file
    attr_reader :line
    attr_reader :column

    attr_reader :details
  end

  class Proxy < Object
    attr_reader :name
  end

  class Docblock < Object
    attr_reader :title
    attr_reader :description

    def has_tag?(kind)
      @tags.has_key?(kind)
    end

    def tag(kind)
      @tags.fetch(kind) { return }.first
    end

    def tags(kind)
      @tags.fetch(kind, [])
    end

    def each_tag(kind, &block)
      @tags.fetch(kind) { return self }.each(&block)

      self
    end

    def has_tags?
      !@tags.empty?
    end

    def has_param?(name)
      @params.has_key?(name)
    end

    def param(name)
      @params[name]
    end
  end

  class Tag < Object
    attr_reader :title
    attr_reader :kind
    attr_reader :description
  end

  class Include < Object
    attr_reader :id
    attr_reader :file

    def local?
      @local
    end
  end

  class Ref < Object
    attr_reader :id
    attr_reader :visibility
    attr_reader :tooltip
    attr_reader :name
  end

  class CompoundRef < Ref
    attr_reader :virtuality
  end

  class Compound < CodeObject
    attr_accessor :raw_content

    # e.g. 'cassandra.h' or 'CassBytes'
    attr_reader :name

    # :class, :struct, :union, :interface, :protocol, :category, :exception,
    # :service, :singleton, :module, :type, :file, :namespace, :group, :page,
    # :example or :dir - overloaded by children
    attr_reader :kind

    # :public, :protected, :private or :package
    attr_reader :visibility

    # @return [Array<Compound>] - list of parents
    attr_reader :parents

    # @return [Array<Compound>] - list of children
    attr_reader :children

    attr_reader :includes

    attr_reader :includedby

    attr_reader :dirs

    attr_reader :files

    attr_reader :classes

    attr_reader :namespaces

    attr_reader :pages

    attr_reader :groups

    def final?
      @final
    end

    def sealed?
      @sealed
    end

    def abstract?
      @abstract
    end

    def each_section(&block)
      @sections.each(&block)
      self
    end
  end

  class Value < Object
    attr_reader :id
    attr_reader :name
    attr_reader :initializer
    attr_reader :details
  end

  class Param < Object
    attr_reader :name
    attr_reader :type
  end

  class Parameter < Object
    attr_reader :name
    attr_reader :direction
    attr_reader :description
  end

  class Member < CodeObject
    attr_reader :kind
    attr_reader :type
    attr_reader :initializer
    attr_reader :exceptions
    attr_reader :definition
    attr_reader :argsstring
    attr_reader :name
    attr_reader :params
    attr_reader :values
    attr_reader :visibility
    attr_reader :static
    attr_reader :const
    attr_reader :explicit
    attr_reader :inline
    attr_reader :virt
    attr_reader :volatile
    attr_reader :mutable
    attr_reader :readable
    attr_reader :writable
    attr_reader :initonly
    attr_reader :settable
    attr_reader :gettable
    attr_reader :final
    attr_reader :sealed
    attr_reader :new
    attr_reader :add
    attr_reader :remove
    attr_reader :raise

    def each_param(&block)
      @params.each(&block)

      self
    end
  end
end
