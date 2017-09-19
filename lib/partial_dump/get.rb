require 'docile'
require 'pg'

require_relative 'formatters'

# Utilities for dumping parts of tables and databases.
# See {PartialDump.get} and {PartialDump.manifest}.
module PartialDump
  # Columns in the dump are sorted alphabetically, with the id column first.
  # UpdateFormatter DEPENDS ON THIS! I realise that's a bit of a hack but I
  # coded myself into a corner and I've spent enough time on this for now.
  COLUMN_ORDERING = lambda do |a, b|
    case
    when a == b
      0
    when a == 'id'
      -1
    when b == 'id'
      1
    else
      a <=> b
    end
  end

  # Extracts rows from the given table matching the given SQL condition
  # on the given connection, and produces a String suitable for use as
  # an SQL dump.
  #
  # By default a postgres-specific COPY command is used, since it seems
  # to be the most efficient in most cases. A number of options are
  # available via the +:type+ option:
  # +:copy+::
  #   Generates a postgreSQL COPY statement.
  # +:insert+::
  #   Generates a single INSERT statement with all
  #   tuples included in a comma separated list.
  # +:inserts+::
  #   Generates one INSERT statement per tuple. Bulky
  #   but useful if some inserts are expected to fail.
  # +:updates+::
  #   Generates one UPDATE statement per tuple, updating
  #   rows with matching IDs to the dumped values.
  #
  # Other options that can be supplied via the +options+ parameter:
  # +:omit_ids+ {Enumerable<Integer>}::
  #   Do not include IDs, so that they will be generated
  #   by the table's default sequence.
  # +:omit_columns+ {Enumerable<#to_s>}::
  #   Do not include the named columns in the dump.
  # +:delete_first+ +true+::
  #   First delete all records with the same IDs as the ones dumped
  # +:columns+ {Enumerable<#to_s>}::
  #   A list or set that names the columns to be dumped. Useful for
  #   +:updates+ mode, or when there are default column values.
  # +:substitutions+ {Hash<column,value>}::
  #   A map of column names, along with the value that should be used
  #   for each column in place of the actual value in the database.
  #   If the value is a proc-like object (responds to call()), it
  #   will be passed the original value, so that it may be
  #   transformed.
  # +:transaction+ [:full,:begin,nil]::
  #   Should be :full to wrap the dump in a transaction, or :begin to
  #   put a BEGIN statement at the start with no matching COMMIT.
  def self.get(conn, table, condition, options = {})
    dump = get_with_ids(conn, table, condition, options).first

    dump
  end

  # Exactly the same as {PartialDump.get}, except that instead
  # of returning only a dump string, +get_with_ids+ returns a tuple
  # containing two items; the dump string, and an array of the
  # IDs of the rows returned in the dump.
  #
  # @return [ [ String, Array<Integer> ] ]
  #   the dump, plus the IDs of the tuples in the dump
  def self.get_with_ids(conn, table, condition, options = {})
    validate_options! options

    # Pull down the data to dump
    data = conn.exec "SELECT * FROM #{table} WHERE #{condition}"
    return [nil, []] if data.num_tuples == 0

    # Extract ids
    ids = data.map { |row| row['id'].to_i }

    # Work out which columns to dump,
    # based on whitelist options[:columns]
    keys = data[0].keys
    keys.delete('id') if options[:omit_ids]
    if options[:columns]
      options[:columns] << 'id'
      keys = keys.select { |k| options[:columns].include? k }
    end

    # ... and blacklist options[:omit_columns]
    if options[:omit_columns]
      keys = keys.reject { |k| options[:omit_columns].include? k }
    end

    # The order that columns will appear in the dump
    keys.sort!(&COLUMN_ORDERING)

    # Collect a table of results,
    # with substitutions and SQL formatting applied to all values.
    values = data.collect do |row|
      substitute_values! row, options[:substitutions]
      keys.collect { |key| options[:formatter].escape row[key] }
    end

    # Generate the dump string
    dump = ''
    dump << "BEGIN;\n\n" if options[:transaction]
    dump << clear_ids(table, data) if options[:delete_first]
    dump << options[:formatter].format(table, keys, values)
    dump << "\n\nCOMMIT;" if options[:transaction] == :full

    # Returning a two-valued array
    [dump, ids]
  end

  protected

  # Ensures that valid values are provided for options
  # and supplies defaults. Raises errors if anything is amiss.
  #
  # @param options [Hash] the +options+ argument to {PartialDump.get}
  def self.validate_options!(options)
    options[:type] ||= :copy
    unless Formatters::DUMP_TYPES.include? options[:type]
      fail ArgumentError, "Invalid dump type: #{options[:type]}"
    end
    options[:formatter] = Formatters::DUMP_FORMATTERS[options[:type]]

    # Anything except false or nil is considered true for booleans
    options[:omit_ids] = options[:omit_ids]
    options[:delete_first] = options[:delete_first]

    if options[:columns] && !options[:columns].is_a?(Array)
      fail ArgumentError, "Wanted Array, got #{options[:columns].class}"
    end

    if options[:type] == :updates
      if options[:omit_ids]
        fail ArgumentError, 'Option omit_ids not valid for update dumps'
      end

      if options[:delete_first]
        fail ArgumentError, 'Option delete_first not valid for update dumps'
      end
    end

    if options[:transaction] && ![:begin, :full].include?(options[:transaction])
      fail(
        ArgumentError,
        "Invalid transaction type given: #{options[:transaction]}"
      )
    end

    options[:substitutions] ||= {}
    unless options[:substitutions].class == Hash
      fail ArgumentError, "Wanted Hash, got #{options[:substitutions].class}"
    end
  end

  # Performs substitutions as specified in the partial dump's options
  # on a tuple as returned by the database query. The tuple will be
  # modified directly.
  #
  # @param row [Hash] A hash representing a tuple in the result set.
  # @param substitutions [Hash] A hash representing substitutions to be made
  #                             on the result set.
  def self.substitute_values! row, substitutions
    substitutions.each do |key, value|
      if value.respond_to? :call
        row[key] = value.call(row[key])
      else
        row[key] = value
      end
    end
  end

  # Generates an SQL statement to DELETE data
  # from the database before restoring from the dump.
  #
  # @param table [#to_s] name of the table
  # @param data [Enumerable<Hash<String,String>>] pg result set of dumped data
  # @return [String]
  #   an SQL DELETE statement matching the IDs for the dumped data
  def self.clear_ids(table, data)
    ids = data.collect { |row| row['id'] }.join ','
    "DELETE FROM #{table} WHERE id IN (#{ids});\n\n"
  end
end

# Creates an SQL dump that can be used to re-create
# the data from the given table that matches the given condition.
# See {PartialDump.get} for more details.
def get_partial_dump(conn, table, condition, options = {})
  PartialDump.get conn, table, condition, options
end
