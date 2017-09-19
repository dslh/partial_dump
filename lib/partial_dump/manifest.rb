require_relative 'get'

# Utilities for dumping parts of tables and databases.
# See {PartialDump.get} and {PartialDump.manifest}.
module PartialDump

  # Defines methods that can be executed from the block
  # passed to {PartialDump.manifest}.
  # Writes all files to the current working directory.
  class ManifestScope
    # Create a new scope. Should only be called
    # by {PartialDump.manifest}.
    #
    # @param conn [PGconn] database connection to pull data from
    # @param header [String] header to be written to the master file
    def initialize conn, header
      @conn = conn
      @master = File.open "all.sql", 'w'

      db_version = conn.query(
          "SELECT max(version) FROM schema_migrations"
        )[0]['max']

      @master.puts "--%%VERSION=#{db_version}"
      @master.puts header
      @master.puts 'BEGIN;'

      @master.puts "-- psql will exit with code 3"
      @master.puts "-- if this script fails"
      @master.puts "\\set ON_ERROR_STOP 1\n"

      @started = Time.now
    end

    # Declare a table that should be dumped as
    # a part of the manifest. By default all data
    # will be dumped, and the data will be written
    # to an .sql file that has the same name as the
    # table.
    #
    # The option +:as+ can be provided, in
    # which case the file will be named +<as>.sql+.
    # This is necessary if a manifest dumps two
    # parts of the same table.
    #
    # Other options will be passed to {PartialDump.get}.
    #
    # A block may be passed to +dump+, in which case
    # the function will yield a string suitable for
    # use in SQL statements as an array containing
    # the IDs of all dumped rows. For example:
    #
    #  dump(:vehicle,'companyId = 209') do |vehicles|
    #    dump(:day,"vehicleId IN #{vehicles}")
    #  end
    #
    # The contents of the string are not guaranteed;
    # it may either be a list of integers or an
    # equivalent bracketed SQL statement.
    #
    # @param table [String] the name of the table to dump, may include schema.
    # @param condition [#to_s] everything after +WHERE+ in the SQL statement. Should be used to declare which parts of the table to dump, but may also be used to +ORDER+ the dump.
    # @param options [Hash] optional arguments. See above.
    def dump table, condition = true, options = {}
      as = options[:as] || table
      as = "#{as}.sql"

      dump, ids = PartialDump.get_with_ids @conn, table, condition, options
      if dump
        File.open(as, 'w') { |f| f.puts dump }
        @master.puts "\\i ./#{as}"
        $stderr.puts "#{dump.lines.count} lines written to #{as}"
      else
        $stderr.puts "No rows returned: SELECT * FROM #{table} WHERE #{condition}"
      end

      if block_given?
        yield "(#{ids.join ','})"
      end
    end

    # Declare a reference to a static .sql file,
    # that has been hand-written or otherwise
    # obtained outside the manifest, that should
    # be executed at this point during dump restoration.
    # This function will write a postgres-specific
    # command to the master manifest dump file.
    #
    # @param file [String] relative path to the file to be included.
    def include file
      @master.puts "\\i #{file}"
    end

    # Declares an arbitrary SQL command
    # that should be executed at this point
    # in the manifest restore process.
    #
    # The SQL will be written directly
    # to the master manifest file.
    #
    # @param sql [String] an ordinary SQL statement
    def sql sql
      @master.puts "#{sql};"
    end

    # Given one or more table names,
    # adds SQL to the master manifest dump
    # so that the corresponding +<table>_id_seq+
    # database sequence will be reset so that
    # the current value exceeds the maximum ID
    # value currently stored in each table.
    #
    # Should be called after any #dump declarations
    # for the table(s), usually towards the end of
    # the manifest.rb file. Assumes default naming
    # standards have been used for the sequence.
    #
    # @param tables [Array<String>] one or more tables that should have their IDs reset.
    def reset_id_seq *tables
      tables.each do |table|
        sql "SELECT setval('#{table}_id_seq',max(id)) FROM #{table}"
      end
    end

    # Writes a footer to the master manifest file
    # and closes the manifest. This function is
    # called automatically and should not be
    # called directly from the manifest block.
    def close
      @master.puts "COMMIT;"
      @master.puts "-- Generated in #{Time.now - @started} seconds"
      @master.close
    end
  end

  # Allows a dump manifest to be declared as a block, to
  # streamline generation of dumps from multiple tables.
  # Dumps will be stored as individual files, along with
  # a master file all.sql which will execute them in the
  # correct order.
  #
  # == Parameters:
  # conn::
  #   A database connection that will be used to retrieve data.
  # header::
  #   A header that will be added to al generated files.
  # dir::
  #   The directory to which all generated files should be saved.
  #   By default this will be the same directory as the manifest.
  #
  # == Manifest DSL:
  # A block should be provided, in which the following
  # commands are available to describe the data dump that
  # should be created. A live database connection is used,
  # so be aware that the manifest isn't fully validated
  # before the first commands are executed.
  #
  # One +.sql+ file will be generated for each +dump+
  # declared, and a master manifest dump file +all.sql+
  # will be written in the same directory. Executing this
  # file using the +psql+ command line app will cause the
  # entire dump to be restored (there are one or two rake
  # tasks in the core repository's +schema/+ directory that
  # can help). The manifest will be dumped in the order it
  # was declared, so be aware of foreign key dependencies
  # when deciding the order to dump tables.
  #
  # See PartialDump::Manifest for more details on each command.
  # The available commands are:
  # dump ({PartialDump::ManifestScope#dump})::
  #   Dumps data from the given table that matches the
  #   given condition, and writes it to the file <table>.sql.
  #   A command will be written to the master file, so that
  #   this file will be loaded when the master file is run.
  #   The connection passed to the manifest will be used.
  #   Options are the same as those available on PartialDump.get,
  #   with the addition of :as, which when present will cause the
  #   dump to be written to <as>.sql instead.
  #
  # sql ({PartialDump::ManifestScope#sql})::
  #   Adds an arbitrary sql command to the master file so that
  #   it will be executed when the dumps are restored. No
  #   validation is done on the given sql string but a
  #   semicolon does get added to the end so that's nice.
  #
  # include ({PartialDump::ManifestScope#include})::
  #   Adds a reference to the given sql file to the master file,
  #   so that the file will be loaded inline with the rest of
  #   the data that is being restored. Filename given should be
  #   relative to the output directory.
  #
  # reset_id_seq ({PartialDump::ManifestScope#reset_id_seq})::
  #   Given a list of one or more table names, will generate SQL
  #   that resets the ID sequence for each table to the current
  #   maximum ID value in the table. Assumes that the table's ID
  #   sequence has the default name of table_name_id_seq.
  #   Should generally be called towards the end of a manifest
  #   once all the data has been dumped.
  def self.manifest conn, header = "Generated by ./#{File.basename $0} at #{Time.now}", dir = File.dirname($0), &block
    # Manifest files kept in cucumber feature/support directories will
    # be automatically included when the tests are run. We'd like to avoid that.
    return if defined? Cucumber

    # All filenames are relative to the target directory.
    # We switch to that directory, then ensure we switch
    # back afterwards.
    wd = Dir.getwd
    begin
      Dir.chdir dir

      # Create a ManifestScope object, as defined above, and execute the block
      # as if it was an instance method of the object. The block can then execute
      # any methods defined in the object. This creates the DSL syntax.
      Docile.dsl_eval(ManifestScope.new(conn, "-- #{header}"), &block).close
    ensure
      Dir.chdir wd
    end
  end

end
