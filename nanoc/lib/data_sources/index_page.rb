# encoding: utf-8

module Docs
  class IndexPageDataSource < Nanoc::DataSource
    identifier :index_page

    def items
      [
        Nanoc::Item.new(File.read('README.md'), {
          :title     => 'Introduction',
          :summary   => 'Home Page <small class="text-muted">page</small>',
          :extension => 'md'
        }, '/')
      ]
    end
  end
end
