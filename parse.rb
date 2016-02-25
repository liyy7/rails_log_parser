#!/usr/bin/env ruby

require 'bundler/setup'

require 'elasticsearch'
require 'pry'

MSG_TYPES = %w(DEBUG INFO WARN ERROR FATAL UNKNOWN)
MSG_TYPE_PREFIXES = MSG_TYPES.map { |t| t[0] }
MSG_REGEX = %r/[#{MSG_TYPE_PREFIXES.join}], \[(?<t>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+) #\d+\]\ {1,2}(?<msg_type>#{MSG_TYPES.join('|')}) -- : (?<msg>.+)/

class Message
  attr_reader :time, :type

  def initialize(ts, msg_type, msg)
    @time = Time.parse(ts)
    @type = msg_type
    @msg = msg
  end

  def timestamp
    (@time.to_f * 1000).to_i
  end

  def append(s)
    @msg += s
  end

  def to_s
    @msg
  end
end

def save_message(es_client, index, type, message)
  es_client.index index: index, type: type, body: {
    timestamp: message.timestamp,
    type: message.type,
    message: message.to_s
  }
end

def print_message(message)
  prefix = sprintf('[%s] %7s -- : ', message.time, message.type)
  msgs = message.to_s.split("\n")
  puts "#{prefix}#{msgs[0]}"
  msgs[1..-1].each { |msg| puts "#{' ' * prefix.size}#{msg}" }
end

def parse(file)
  current_message = nil

  File.open(file).each do |line|
    line.strip!
    matched = MSG_REGEX.match(line)
    if matched.nil?
      # If it is not a new line of log, append the whole line to current message.
      current_message.append("\n" + line) unless current_message.nil?
      next
    end

    # Got a new line of log, yield current message.
    yield(current_message) if !current_message.nil? && block_given?

    current_message = Message.new(matched['t'], matched['msg_type'], matched['msg'])
  end

  # If current message exists, yield it.
  yield(current_message) if !current_message.nil? && block_given?
end

def recreate_index(es_client, index, type)
  es_client.indices.delete(index: index) if es_client.indices.exists?(index: index)
  es_client.indices.create index: index, body: {
    settings: {
      index: {
        number_of_shards: 1,
        number_of_replicas: 0
      }
    },
    mappings: {
      type => {
        properties: {
          timestamp: {
            type: 'date'
          }
        }
      }
    }
  }
end

# How to execute:
#  ./parse.rb log/production.log
if __FILE__ == $0
  es_client = Elasticsearch::Client.new
  index = 'rails_log'
  type = 'rails_log'

  recreate_index(es_client, index, type)

  parse(ARGV.first) do |message|
    save_message(es_client, index, type, message)
    print_message(message)
  end
end
