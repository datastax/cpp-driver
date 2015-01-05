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
          title      = base.last.split('_').map(&:capitalize).join(' ')
          kind       = 'section'
          identifier = base.join('/')
          type       = :section
        else
          next
        end

        items << Nanoc::Item.new(File.read(path), {
          :title     => title,
          :extension => ext,
          :type      => type,
          :summary   => "#{title} <small class=\"text-muted\">#{kind}</small>"
        }, identifier)
      end

      items
    end
  end
end
