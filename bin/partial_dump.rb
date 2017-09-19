#!/usr/bin/env ruby

require 'bundler/setup'
require 'partial_dump'
require 'trollop'

cl_opts = Trollop.options do
  banner <<-EOS
Usage: #{__FILE__} [options] table condition
Dumps part of the data from a given table
into an SQL COPY command on stdout. The
'condition' parameter should be in standard
SQL and can include an ORDER BY clause, i.e.
it is everything after 'WHERE' in an SQL query.

  examples:
#{__FILE__} vehicles companyId=209
#{__FILE__} day "date > '2015-01-01' AND vehicleId IN (
  SELECT id FROM Vehicle WHERE companyId = 209
)"

  options:
  EOS

  opt :db, 'Database to dump from', type: :string
  opt :host, 'Database server. Defaults to unix socket.', type: :string
  opt :port, 'Database port', default: 5432
  opt :user, 'Database user', default: ENV['USER']
  opt :pass, 'Database password', default: 'trusted'
  opt :verbose, 'Print information to stderr'
  opt :updates, 'Dump will UPDATE existing rows by ID, rather than INSERT.'
  opt :inserts, 'Generate INSERT statements rather than a COPY statement.'
  opt :insert, 'Generate a single INSERT statement instead of a COPY statement.'
  opt :omit_id, 'Use database sequences for IDs rather than declaring them.'
  opt :omit_columns, 'Use default value for given columns.'
  opt :delete_first, 'Prepend DELETE to dump, for clear-and-restore behaviour.'
  opt :transaction, 'Wrap the dump in a transaction.'
  opt :begin_transaction, 'Generate a BEGIN but no COMMIT, for testing.'
  opt :columns, 'Dump only these columns (plus ID)', type: :strings
  opt(
    :substitutions,
    'Replace given fields with given values, e.g. "companyId=1"',
    type: :strings
  )
end
Trollop.die 'must supply table name and conditional SQL' if ARGV.size < 2
Trollop.die 'too many arguments' if ARGV.size > 2
Trollop.die 'must specify database to dump from' unless cl_opts[:db]
if cl_opts[:insert] && cl_opts[:inserts]
  Trollop.die 'do you want a single INSERT or multiple?'
end
if (cl_opts[:inserts] || cl_opts[:insert]) && cl_opts[:updates]
  Trollop.die 'do you want INSERTs or UPDATEs?'
end
if cl_opts[:transaction] && cl_opts[:begin_transaction]
  Trollop.die 'do you want a full or partial transaction?'
end

substitutions = cl_opts[:substitutions]
if substitutions
  unless substitutions.all? { |s| s['='] }
    Trollop.die 'invalid substitution supplied, should be of the form f=v'
  end

  substitutions.map! { |pair| pair.split '=' }
  substitutions = Hash[*substitions.flatten.collect { |s| s.strip.downcase }]
else
  substitutions = {}
end

table = ARGV[0]
condition = ARGV[1]

# Work out which type of dump has been specified.
DUMP_TYPES = [:insert, :inserts, :updates, :copy]
type = DUMP_TYPES.find { |t| cl_opts[t] } || :copy

transaction =
  case
  when cl_opts[:begin_transaction]
    :begin
  when cl_opts[:transaction]
    :full
  end

if cl_opts[:verbose]
  $stderr.puts(
    'postgres://%s@%s:%i/%s',
    cl_opts[:user],
    (cl_opts[:host] || '<socket>'),
    cl_opts[:port],
    cl_opts[:db]
  )
end

conn = PGconn.connect(
  cl_opts[:host],
  cl_opts[:port],
  '',
  '',
  cl_opts[:db],
  cl_opts[:user],
  cl_opts[:pass]
)

$stderr.puts "SELECT * FROM #{table} WHERE #{condition}" if cl_opts[:verbose]

options = {
  type: type,
  omit_ids: cl_opts[:omit_id],
  delete_first: cl_opts[:delete_first],
  columns: cl_opts[:columns],
  substitutions: substitutions,
  transaction: transaction
}

dump = get_partial_dump(conn, table, condition, options)

unless dump
  $stderr.puts 'Dump is empty!'
  exit(-1)
end

puts dump
