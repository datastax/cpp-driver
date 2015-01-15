# encoding: utf-8

module Docs
  class TopicsDataSource < Nanoc::DataSource
    identifier :topics

    def topics_dir_name
      config.fetch(:topics_dir, 'topics')
    end

    def items
      items = []

      glob  = '**/*.*'
      dir   = topics_dir_name
      glob  = [dir, glob].join('/') unless dir.empty?

      Dir[glob].each do |path|
        *base, filename = path.split('/')
        *filename, ext  = filename.split('.')
        filename = filename.join('.')

        if filename == 'README'
          title = base.last.split('_').map do |x|
            if ['ssl', 'faq' ].include? x
              x.upcase
            else
              x.capitalize
            end
          end

          title      = title.join(' ')
          kind       = 'section'
          identifier = base.join('/')
          type       = :section
        else
          next
        end

        content = File.read(path)

        items << Nanoc::Item.new(content, {
          :title     => title,
          :extension => ext,
          :text      => content,
          :type      => type,
          :summary   => "#{title} <small class=\"text-muted\">#{kind}</small>",
          :mtime     => File.mtime(path)
        }, identifier)
      end

      items
    end
  end
end
