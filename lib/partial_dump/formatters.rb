
# Utilities for dumping parts of tables and databases.
# See {PartialDump.get} and {PartialDump.manifest}.
module PartialDump

  # The partial_dump library can output database dumps
  # in a few different forms of SQL statement; COPY,
  # INSERT, UPDATE. This module contains code for
  # generating these different formats
  module Formatters

    # Base class for formatters, which take data dumped
    # from a table and format it as a string which can be
    # used to reload the data.
    #
    # All formatters should be immutable.
    class SqlFormatter

      # Escapes, quotes and handles nulls for values,
      # suitable for an SQL statement.
      #
      # @param value [String] an SQL value to be escaped.
      # @return [String] and escaped SQL string.
      def escape(value)
        if value.nil?
          "NULL"
        else
          "'#{value.to_s.gsub(/'/,"''")}'"
        end
      end

      # Format a +PGresult+ object or similar data structure,
      # as opposed to the array-of-arrays used for values by
      # the +format()+ function. See {Formatters.as_value_table}
      # for more details.
      #
      # "key" (row name) values can be strings or symbols,
      # as long as they match between the +keys+ and +values+
      # parameters.
      #
      # @param [#to_s] table name of the table being dumped to
      # @param [Array<key>] keys an array of row names that will be dumped
      # @param [Enumerable<Hash<key,value>>] results [PGresult] object or similar
      # @return [String] an SQL INSERT/COPY/UPDATE statement
      def format_results table, keys, results
        format(table, keys, Formatters.as_value_table(keys, results))
      end
    end

    # Formats dumped data as a PostgreSQL COPY command.
    class CopyFormatter < SqlFormatter
      # Escapes, quotes and handles nulls for values,
      # suitable for a PostgreSQL COPY statement.
      #
      # @param value [String] an SQL value to be escaped.
      # @return [String] and escaped SQL string.
      def escape(value)
        if value.nil?
          "\\N"
        else
          value.to_s.gsub(/\r/,"\\r").gsub(/\n/,"\\n").gsub(/\t/, "\\t").gsub(/\\/, "\\\\")
        end
      end

      # Format a result set as a postgres COPY statement.
      #
      # @param table [String] database table name
      # @param keys [Enumerable<#to_s>] a list of column names being dumped
      # @param values [Enumerable<Enumerable<#to_s>>] a table of data, with values for each row in the same order as the +keys+ parameter.
      # @return [String] a string that can be executed as SQL
      def format table, keys, values
        <<-SQL
COPY #{table} (#{keys.join(', ')}) FROM stdin;
#{values.collect { |row| row.join "\t" }.join "\n"}
\\.
        SQL
      end

    end

    # Produces a single SQL INSERT command, with multiple
    # comma separated value tuples.
    class SingleInsertFormatter < SqlFormatter

      # Format a result set as a compound INSERT statement.
      #
      # @param table [String] database table name
      # @param keys [Enumerable<#to_s>] a list of column names being dumped
      # @param values [Enumerable<Enumerable<#to_s>>] a table of data, with values for each row in the same order as the +keys+ parameter.
      # @return [String] a string that can be executed as SQL
      def format table, keys, values
        "INSERT INTO #{table} (#{keys.join(', ')}) VALUES
      #{values.collect { |row| "(#{row.join(',')})" }.join(",\n    ")};"
      end

    end

    # Produces one SQL INSERT command per tuple, useful if
    # some lines are expected to fail.
    class MultipleInsertFormatter < SqlFormatter

      # Format a result set as a set of INSERT statements.
      #
      # @param table [String] database table name
      # @param keys [Enumerable<#to_s>] a list of column names being dumped
      # @param values [Enumerable<Enumerable<#to_s>>] a table of data, with values for each row in the same order as the +keys+ parameter.
      # @return [String] a string that can be executed as SQL
      def format table, keys, values
        values.collect do |row|
          "INSERT INTO #{table} (#{keys.join(',')}) VALUES (#{row.join(',')});"
        end.join("\n")
      end

    end

    # Generates UPDATE statements that restore a set of rows
    # to their original values.
    class UpdateFormatter < SqlFormatter

      # Format a result set as a set of UPDATE statements.
      #
      # @param table [String] database table name
      # @param keys [Enumerable<#to_s>] a list of column names being dumped
      # @param values [Enumerable<Enumerable<#to_s>>] a table of data, with values for each row in the same order as the +keys+ parameter.
      # @return [String] a string that can be executed as SQL
      def format table, keys, values
        values.collect do |row|
          assignments = (1...keys.size).collect { |i| "#{keys[i]}=#{row[i]}" }.join(', ')
          "UPDATE #{table} SET #{assignments} WHERE id=#{row[0]};"
        end.join "\n"
      end

    end

    # Aliases for the available formatters
    DUMP_TYPES = [:copy, :insert, :inserts, :updates]

    # Mapping of aliases to implementations.
    # Used for command-line parsing.
    # Formatters are all immutable.
    DUMP_FORMATTERS = {
      :copy => CopyFormatter.new,
      :insert => SingleInsertFormatter.new,
      :inserts => MultipleInsertFormatter.new,
      :updates => UpdateFormatter.new
    }

    # Helper function which converts from a PGresult object,
    # or similar data structure, to an array of arrays of values
    # as expected for the +values+ parameter of any of the +format()+
    # functions.
    #
    # The given list of keys can be either strings or symbols,
    # but must match the type of the keys in the array of hashes.
    #
    # @param [Array<key>] keys the names of the table rows that will be dumped
    # @param [Enumerable<Hash<key,value>>] values the +PGresult+ object, or similar
    # @return [Array<Array<value>>] a table of values, with each row in the same order as the given list of keys
    def self.as_value_table keys, values
      values.map do |row|
        keys.map { |key| row[key] }
      end
    end

  end

end

