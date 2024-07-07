#!/usr/bin/env perl
use strict;
use warnings;
use Time::HiRes qw(time);

# Record the start time for performance measurement
my $start_time = time();

# Define input and output file names
my $original_data_file = 'original_data.sql';
my $converted_data_file = 'converted_data.sql';
my $debug_file = 'debug.sql';

# Remove output files if they already exist
unlink $converted_data_file;
unlink $debug_file;

sub process_data {
	my ( $original_data_file, $converted_data_file, $debug_file ) = @_;

	# Open files for reading and writing
	open my $original_data, '<:raw', $original_data_file
	  or die "Unable to open file: $!";
	open my $processed_data, '>:raw', $converted_data_file
	  or die "Unable to open file: $!";
	open my $debug_data, '>:raw', $debug_file or die "Unable to open file: $!";

	# Define processing parameters
	my $chunk_size = 1 * 1024 * 1024;
	my $total_bytes_read = 0;
	my $total_bytes_written = 0;
	my $unprocessed_data = '';
	my $current_line_number = 0;

	# Specific line numbers to debug
	my %lines_to_debug = map { $_ => 1 } (1148);

	# Main processing loop
	while (1) {

		# Read a chunk of data
		my $chunk;
		my $bytes_read = read( $original_data, $chunk, $chunk_size );

		# Check for read errors or end of file
		if ( !defined $bytes_read ) {
			die "Error reading file: $!";
		}
		elsif ( $bytes_read == 0 ) {
			last;
		}

		$total_bytes_read += $bytes_read;
		$unprocessed_data .= $chunk;

		# Process data line by line
		while ($unprocessed_data =~ s/^(INSERT.*?\n)//s
			|| $unprocessed_data =~ s/^.*?\n//s )
		{
			my $line = $1;

			if ( $line && $line =~ /^INSERT/i ) {
				$current_line_number++;

				# Perform data transformations
				$line =~ s/\\'/\'\'/g;
				$line =~ s/\\\'\'/\'\'/g;
				$line =~ s/\\{2,}/\\/g;
				$line =~ s/\\'\'/\'\'/g;

				# Debug specific lines if needed
				if ( exists $lines_to_debug{$current_line_number} ) {
					print $debug_data "$line";
					print STDERR "Extracted INSERT line $current_line_number\n";
				}

				# Write processed line to output file
				my $bytes_written = print $processed_data $line;
				if ( !defined $bytes_written ) {
					die "Error writing to file: $!";
				}
				$total_bytes_written += $bytes_written;
			}
		}

		# Print progress every 100 MB
		if ( $total_bytes_read % ( 100 * 1024 * 1024 ) == 0 ) {
			my $mb_processed = $total_bytes_read / ( 1024 * 1024 );
			print STDERR "Processed $mb_processed MB\n";
		}
	}

	# Print final statistics
	my $mb_read = $total_bytes_read / ( 1024 * 1024 );
	my $mb_written = $total_bytes_written / ( 1024 * 1024 );
	print STDERR "Finishing process...\n";
	print STDERR "Total INSERT lines processed: $current_line_number\n";

	# Close all file handles
	close $original_data;
	close $processed_data;
	close $debug_file;

	return;
}

# Execute the data processing function and handle any errors
eval { process_data( $original_data_file, $converted_data_file, $debug_file ); };
if ($@) {
	die "An error occurred: $@";
}

# Print completion message and execution time
print "Done\n";
my $end_time = time();
my $time_taken = $end_time - $start_time;
printf "Time taken: %.2f seconds\n", $time_taken;
