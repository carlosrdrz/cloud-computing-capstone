#!/usr/bin/ruby

require 'parallel'
require 'zip'
require 'csv'

source_dir = ARGV[0]
dest_dir = ARGV[1]

# Keys to select from the CSV files
keys = %w(FlightDate DayOfWeek FlightNum Origin Dest DepDelayMinutes ArrDelayMinutes UniqueCarrier)

files = Dir["#{source_dir}/**/*.zip"]
Parallel.map(files, in_processes: 38) do |file|
  puts file

  begin
    # Uncompressing zip file
    Zip::File.open(file) do |zip_file|
      zip_file.glob('*.csv').each do |csv_file_entry|
        csv_file = csv_file_entry.get_input_stream.read
      
        # Opening CSV file
        File.open("#{dest_dir}/#{csv_file_entry.name}", 'a') do |f|
          CSV.parse(csv_file, headers: true) do |row|
            # Ignoring cancelled or diverted flights since they never make
            # it to their destination
            cancelled = row.fetch('Cancelled', '0.0').to_i
            diverted = row.fetch('Diverted', '0.0').to_i
            next if cancelled == 1 || diverted == 1
            values = keys.map { |k| row[k] }
            f.write "#{values.join(',')}\n"
          end
        end
      end
    end
  rescue StandardError => e
    # There was some kind of issue decompressing the
    # zip file or reading the CSV file
    puts "[ERROR] #{file} #{e.message}"
  end
end

