#!/usr/bin/ruby
# coding: utf-8

require 'aws-sdk'
require 'optparse'
require 'date'
require 'fileutils'

def init_options
  options = {}

  opt = OptionParser.new
  opt.on('-t DATETIME', '--datetime=DATETIME', 'Target log last modified datetime') do |v|
    options[:datetime] = DateTime.parse(v)
  end

  opt.on('-r REGION', '--region=REGION', 'Target RDS region') do |v|
    options[:region] = v
  end

  opt.on('-i INSTANCE', '--instance=INSTANCE', 'Target RDS instance identifier') do |v|
    options[:instance] = v
  end

  opt.on('-d DIR', '--logdir', 'Log output directory') do |v|
    options[:logdir] = v
  end

  opt.on('-k ACCESS_KEY_ID', '--access-key-id=ACCESS_KEY_ID', 'access key id') do |v|
    options[:access_key_id] = v
  end

  opt.on('-s SECRET_ACCESS_KEY', '--secret-access-key=SECRET_ACCESS_KEY', 'secret access key') do |v|
    options[:secret_access_key] = v
  end

  opt.on('-p PROFILE', '--profile=PROFILE', 'Use a specific profile from your credential file.') do |v|
    options[:profile] = v
  end

  begin
    opt.parse!
  rescue OptionParser::ParseError => e
    $stderr.puts e.message
    exit 1
  end

  options[:datetime] = Date.today unless options[:datetime]
  options[:logdir] = '.' unless options[:logdir]
  options[:region] = 'us-east-1' unless options[:region]

  unless options[:instance]
    $stderr.puts "-i INSTANCE option required"
    exit 1
  end

  if options[:profile]
    provider = AWS::Core::CredentialProviders::SharedCredentialFileProvider.new(
      :profile_name => options[:profile]
    )
    options[:credential_provider] = provider
  end

  options
end

def end_of_day_timestamp(date)
  date.to_time.to_i * 1000
end

def collect_db_log_filenames(instance, last_written)
  marker = '0'
  logfilenames = []
  begin
    resp = @rds.client.describe_db_log_files(
      :db_instance_identifier => instance,
      :file_last_written => end_of_day_timestamp(last_written),
      :marker => marker
    )
    marker = resp[:marker].to_s
    logfilenames.concat(resp[:describe_db_log_files].map {|i| i[:log_file_name] })
  end while(marker != '')
  logfilenames
end

def save_db_log_file(instance, filename, logdir, body)
  filepath = File.join(logdir, instance, filename)
  FileUtils.mkdir_p(File.dirname(filepath))
  open(filepath, 'a+') {|f| f.puts body }
end

def download_db_log_file(server, logfilenames, logdir)
  logfilenames.each do |filename|
    marker = '0'
    body = ''
    begin
      resp = @rds.client.download_db_log_file_portion(
        :db_instance_identifier => server,
        :log_file_name => filename,
        :marker => marker
      )
      marker = resp[:marker].to_s
      body += resp[:log_file_data].to_s
    end while(resp[:additional_data_pending])
    save_db_log_file(server, filename, logdir, body)
  end
end

options = init_options
@rds = AWS::RDS.new(options)
filenames = collect_db_log_filenames(options[:instance], options[:datetime])
download_db_log_file(options[:instance], filenames, options[:logdir])

