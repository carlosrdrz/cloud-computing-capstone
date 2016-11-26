#!/usr/bin/ruby

require 'parallel'
require 'zip'
require 'csv'

source_dir = ARGV[0]
dest_dir = ARGV[1]
keys = ARGV[2].split(',')

files = Dir["#{source_dir}/**/*.zip"]
Parallel.map(files, in_processes: 14) do |file|
  puts file

  begin
    Zip::File.open(file) do |zip_file|
      zip_file.glob('*.csv').each do |csv_file_entry|
        csv_file = csv_file_entry.get_input_stream.read
      
        File.open("#{dest_dir}/#{csv_file_entry.name}", 'a') do |f|
          CSV.parse(csv_file, headers: true) do |row|
            values = keys.map { |k| row[k] }
            f.write "#{values.join(',')}\n"
          end
        end
      end
    end
  rescue StandardError => e
    puts "[ERROR] #{file} #{e.message}"
  end
end

