#!/usr/bin/env perl
use strict;
use warnings;
use Time::HiRes qw(time);
use Digest::MD5 qw(md5_hex);

# Record start time for performance measurement
my $start_time = time();

# Define input and output file names
my $original_schema_file = 'original_schema.sql';
my $converted_schema_file = 'converted_schema.sql';
my $post_process_file = 'run_after.sql';
my $pre_process_file = 'run_before.sql';
my $enum_types_file = 'enum_types.sql';
my $drop_tables_file = 'drop_tables.sql';
my %enum_types;
my $enum_counter = 1;

my $current_table = '';

sub process_schema {
	my ( $original_schema_file, $converted_schema_file, $post_process_file, $pre_process_file, $enum_types_file, $drop_tables_file ) = @_;

	# Open and read the original schema file
	open my $original_schema_data, '<', $original_schema_file
	  or die "Unable to open file: $!";
	my @lines = <$original_schema_data>;
	close $original_schema_data;

	# Open output files
	open my $pre_process_data, '>', $pre_process_file
	  or die "Unable to open file: $!";
	open my $converted_schema_data, '>', $converted_schema_file
	  or die "Unable to open file: $!";
	open my $post_process_data, '>', $post_process_file
	  or die "Unable to open file: $!";
	open my $enum_types_data, '>', $enum_types_file
	  or die "Unable to open file: $!";
	open my $drop_tables_data, '>', $drop_tables_file
	  or die "Unable to open file: $!";

	# Initialize arrays to store processed lines
	my @post_process_lines;
	my @pre_process_lines;
	my @converted_lines;
	my @enum_lines;
	my @drop_enum_lines;
	my @drop_table_lines;

	# Process each line of the original schema
	for my $line (@lines) {

		# Skip comments
		next if $line =~ /^\s*(?:--|\/\*)/;

		# Detect current table being processed
		if ( $line =~ /CREATE\s+TABLE\s+"([^"]+)"/ ) {
			$current_table = $1;
			push @drop_table_lines, "DROP TABLE IF EXISTS \"$current_table\" CASCADE;\n";
		}

		# Convert auto-increment columns
		if ( $line =~ /^\s*"(\w+)"\s+(int|bigint)\s+NOT\s+NULL\s+AUTO_INCREMENT\s*,/i ) {
			my $column_name = $1;
			$line =~ s/^\s*"$column_name"\s+(int|bigint)\s+NOT\s+NULL\s+AUTO_INCREMENT\s*,/"$column_name" SERIAL,/;

			# Add post-processing steps for auto-increment columns
			push @post_process_lines, "ALTER TABLE \"$current_table\" ALTER COLUMN \"$column_name\" DROP DEFAULT;\n";

			push @post_process_lines, "DROP SEQUENCE ${current_table}_${column_name}_seq CASCADE;\n";

			push @post_process_lines, "ALTER TABLE \"$current_table\" ALTER COLUMN \"$column_name\" ADD GENERATED ALWAYS AS IDENTITY;\n";

			push @post_process_lines, "SELECT setval('${current_table}_${column_name}_seq', (SELECT COALESCE(MAX(\"$column_name\"), 1) FROM \"$current_table\"));\n\n";
		}

		# Remove MySQL-specific collation
		$line =~ s/\s*COLLATE utf8mb4_general_ci\s*//i;

		# Convert data types
		$line =~ s/\btinyint(?:\(\d+\))?\b/smallint/gi;
		$line =~ s/\bsmallint\(\d+\)/smallint/gi;
		$line =~ s/\blongtext\b/text/gi;
		$line =~ s/"([^"]+)"\s+double/"$1" double precision/gi;
		$line =~ s/\b(tiny|medium|long)text\b/text/gi;
		$line =~ s/\bfloat\b/numeric/gi;
		$line =~ s/\bmediumint\b/integer/gi;

		# Convert datetime and timestamp fields
		$line =~ s/"([^"]+)"\s+datetime\s+NOT\s+NULL\s+DEFAULT\s+'0000-00-00 00:00:00'/"$1" timestamp NULL/gi;
		$line =~ s/"([^"]+)"\s+timestamp\s+NOT\s+NULL\s+DEFAULT\s+CURRENT_TIMESTAMP\s+ON\s+UPDATE\s+CURRENT_TIMESTAMP,/"$1" timestamp NULL,/gi;
		$line =~ s/(\w+)\s+datetime\s+NOT\s+NULL/$1 timestamp NULL/gi;
		$line =~ s/(\w+)\s+datetime\s+DEFAULT\s+NULL/$1 timestamp DEFAULT NULL/gi;
		$line =~ s/DATETIME/TIMESTAMP/gi;

		# Convert date fields
		$line =~ s/(".*?"\s+date\s+)NOT NULL(\s+DEFAULT\s+)'0000-00-00'/$1NULL$2NULL/gi;

		# Convert unique keys to unique indexes
		if ( $line =~ /UNIQUE\s+KEY\s+"([^"]+)"\s+\("([^"]+)"\)/ ) {
			push @post_process_lines, "CREATE UNIQUE INDEX idx_${current_table}_$1 ON \"$current_table\" (\"$2\");\n";
			next;
		}

		# Convert foreign keys
		if ( $line =~ /CONSTRAINT\s+"([^"]+)"\s+FOREIGN KEY\s+\("([^"]+)"\)\s+REFERENCES\s+"([^"]+)"\s+\("([^"]+)"\)(\s+ON DELETE CASCADE)?/ ) {
			my $constraint_name = $1;
			my $foreign_key = $2;
			my $referenced_table = $3;
			my $referenced_column = $4;
			my $on_delete_cascade = $5 ? ' ON DELETE CASCADE' : '';

			push @post_process_lines, "ALTER TABLE \"$current_table\" ADD CONSTRAINT $constraint_name " . "FOREIGN KEY ($foreign_key) REFERENCES $referenced_table ($referenced_column)$on_delete_cascade;\n";
			next;
		}

		# Remove trailing comma from PRIMARY KEY lines
		$line =~ s/,\s*$/\n/ if $line =~ /PRIMARY\s+KEY/;

		# Convert FULLTEXT indexes to comments
		if ( $line =~ /^\s*FULLTEXT\s+KEY\s+"([^"]+)"\s+\("([^"]+)"\)/i ) {
			my $index_name = $1;
			my $column_name = $2;
			push @post_process_lines, "CREATE INDEX idx_fts_${current_table}_$index_name ON \"$current_table\" USING GIN (to_tsvector('english', \"$column_name\"));\n";
			next;
		}

		# Convert regular indexes
		if ( $line =~ /^\s*KEY\s+/i ) {
			if ( $line =~ /KEY\s+"([^"]+)"\s+\("([^"]+)"\)/ ) {
				my $index_name = $1;
				my $column_name = $2;
				push @post_process_lines, "CREATE INDEX idx_${current_table}_$index_name ON \"$current_table\" (\"$column_name\");\n";
			}
			elsif ( $line =~ /KEY\s+"([^"]+)"\s+\("([^"]+)"\((\d+)\)\)/i ) {
				my $index_name = $1;
				my $column_name = $2;
				my $prefix_length = $3;
				push @post_process_lines, "CREATE INDEX idx_${current_table}_$index_name ON \"$current_table\" (LEFT(\"$column_name\", $prefix_length));\n";
			}
			next;
		}

		# Convert ENUM types to VARCHAR
		if ( $line =~ /"([^"]+)"\s+enum\(([^)]+)\)/ ) {
			my $column_name = $1;
			my $enum_values = $2;
			if ( $enum_values !~ /''/ ) {
				$enum_values .= ",''";
			}

			my @items = $enum_values =~ /'([^']*)'/g;

			my $sorted_enums = join( ',', sort @items );

			my $enum_type_name;
			if ( exists $enum_types{$sorted_enums} ) {
				$enum_type_name = $enum_types{$sorted_enums};
			}
			else {
				$enum_type_name = create_enum_type_name($sorted_enums);
				$enum_types{$sorted_enums} = $enum_type_name;

				# Add CREATE TYPE statement to post-processing
				push @enum_lines, "CREATE TYPE $enum_type_name AS ENUM ($enum_values);\n";
				push @drop_enum_lines, "DROP TYPE IF EXISTS $enum_type_name;\n";
			}

			# Replace the line with the new ENUM type
			$line =~ s/enum\([^)]+\)/$enum_type_name/;
		}

		# Simplify timestamp default
		$line =~ s/timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,/timestamp NULL DEFAULT CURRENT_TIMESTAMP,/;

		# Add pre-processing for date/datetime/timestamp fields
		if ( $line =~ /"([^"]+)"\s+(date|datetime|timestamp)/i ) {
			my $field_name = $1;
			push @pre_process_lines, "UPDATE `$current_table` SET `$field_name` = '1970-01-01' WHERE `$field_name` = '0000-00-00';\n";
		}

		# Increase VARCHAR length for fields
		if ( $line =~ /(".*?"\s+varchar\()(\d+)(\))/ ) {
			my $prefix = $1;
			my $length = $2;
			my $suffix = $3;
			if ( $length >= 30 ) {
				my $new_length = $length + 35;
				$line =~ s/(".*?"\s+varchar\()\d+(\))/$1$new_length$2/;
				push @pre_process_lines, "-- Increased VARCHAR length for field in $current_table from $length to $new_length\n";
			}
		}

		push @converted_lines, $line;
	}

	# Write processed lines to respective files
	print $post_process_data @post_process_lines;
	print $pre_process_data @pre_process_lines;
	print $converted_schema_data @converted_lines;
	print $enum_types_data @enum_lines;
	print $drop_tables_data @drop_table_lines;
	print $drop_tables_data @drop_enum_lines;

	# Close all file handles
	close $converted_schema_data;
	close $post_process_data;
	close $pre_process_data;
	close $enum_types_data;
	close $drop_tables_data;

	return;
}

sub sanity_check_schema {
	my ($converted_schema_file) = @_;

	# Read the converted schema file
	open my $converted_schema_data, '<', $converted_schema_file
	  or die "Unable to open file: $!";
	my @check_lines = <$converted_schema_data>;
	close $converted_schema_data;

	# Remove trailing commas before closing parentheses
	for my $i ( 0 .. $#check_lines - 1 ) {
		if ( $check_lines[$i] =~ /,$/ && $check_lines[ $i + 1 ] =~ /\);$/ ) {
			$check_lines[$i] =~ s/,\s*$//;
		}
	}

	# Write the sanitized schema back to the file
	open my $final_schema_data, '>', $converted_schema_file
	  or die "Unable to open file: $!";
	print $final_schema_data @check_lines;
	close $final_schema_data;

	return;
}

sub create_enum_type_name {
	my ($values) = @_;
	my $hash = substr( md5_hex($values), 0, 8 );
	return "enum_${hash}";
}

# Execute the schema conversion process
eval {
	process_schema( $original_schema_file, $converted_schema_file, $post_process_file, $pre_process_file, $enum_types_file, $drop_tables_file );
	sanity_check_schema($converted_schema_file);
};
if ($@) {
	die "An error occurred: $@";
}

# Print completion message and execution time
print "Done\n";
my $end_time = time();
my $time_taken = $end_time - $start_time;
printf "Time taken: %.2f seconds\n", $time_taken;
