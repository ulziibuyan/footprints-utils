#!/usr/footprints_perl/bin/perl --
#
# Copyright 1996-2006 Numara Software Inc.                #COPYRIGHT LINE#
#

use strict;

package FP;

BEGIN
{
    my ($scriptName, $script_dir);

    # current directory
    push @INC, '.';

    # define $FS
    $FP::FS = ($^O =~ /MSWin32/) ? "\\" : "/";

    # Get the directory that the script is in, in case we're being
    # executed from a different directory.

    $scriptName = $0;
    if ( $scriptName =~ /(.*)(\\)(?!\\)/ )    # NT
    {
	$script_dir = $1;
    }
    elsif ( $scriptName =~ /(.*)(\/)(?!\/)/ ) # UNIX
    {
	$script_dir = $1;
    }
    else
    {
	$script_dir = '.';
    }
    
    push @INC, $script_dir if $script_dir ne '.';
    
    push @INC, "..${FP::FS}cgi";
    push @INC, "..${FP::FS}cgi${FP::FS}lib";
    
    # must find AAA.so in current directory for Oracle
    chdir($script_dir) if $script_dir ne '.';
           
    # Add ADDTOINC env var.

    push @INC, $ENV{'ADDTOINC'};

    # Weed out duplicates that might have gotten into @INC -- Perl
    # doesn't know to skip directories that it's already looked in.
    # (be sure to preserve original order of @INC)
    my (%alreadyInINC, @newINC);
    for my $dir (@INC)
    {
	next if $alreadyInINC{$dir};
	
	push @newINC, $dir;
	$alreadyInINC{$dir} = 1;
    }

    @INC = @newINC;

}

require "MRlib.pl";
use DBIx::FP_XML_RDB;
use IO::File;
use Getopt::Long;

####################################################################
## :subs: 
####################################################################

#
# Name     : ISODateWrapper
# Synopsis : If Postgres or Oracle, returns fieldname wrapped in ISO 
#            conversion function. Otherwise just returns the field name.
# Arguments: Field Name
# Returns  : TO_CHAR(fieldname, 'YYYY-MM-DD HH24:MI:SS') or fieldname
# Notes    : None.
#   
sub ISODateWrapper
{
    my ($fieldName, $finalValue);

    $fieldName = $_[0];
    
    if ($FP::ORACLE_DBMS || $FP::POSTGRES_DBMS)
    {
        $finalValue = "TO_CHAR($fieldName, 'YYYY-MM-DD HH24:MI:SS') AS $fieldName";
    }
    else
    {
        $finalValue = $fieldName;
    }
    
    return ($finalValue);
}

#
# Name     : FieldsInTable
# Synopsis : Returns comma seperated list of fields in given table,
#            with dates wrapped in ISO conversion if necessary 
# Arguments: Table Name
# Returns  : Comma seperated list of fields for select statement, these
#            are the fields that will be dumped to the XML file.
# Notes    : The only reason that this is needed is so that dates can be
#            wrapped in an ISO conversion, otherwise a select * would work
#   
sub FieldsInTable
{
    my ($table, @fields, $selectFields, @finalFields);

    $table = $_[0];

    if ($table =~ /^MASTER(\d)+$/)
    {
        my (@schema);

        @fields = ("mrID",
                   "mrREF_TO_AB",
                   "mrREF_TO_MR",
                   "mrTITLE",
                   "mrPRIORITY",
                   "mrSTATUS",
                   "mrDESCRIPTION",
                   "mrALLDESCRIPTIONS",
                   "mrASSIGNEES",
                   "mrATTACHMENTS",
                   &ISODateWrapper("mrUPDATEDATE"),
                   "mrSUBMITTER",
                   &ISODateWrapper("mrSUBMITDATE"),
                   "mrPOPULARITY",
                   "mrUNASSIGNED",
                   "mrURGENT",
                   "mrESCALATEDBY"
                  );

        @schema = &FP::getFieldsFromSchema("$ENV{'CMMASTER'}${FP::FS}MR${FP::FS}Schema");

        foreach my $field (@schema)
        {
            if ($field->{'type'} eq 'date' && $field->{'flag'} >= 0)
            {
                push(@fields, &ISODateWrapper($field->{'name'}));
            }
            elsif ($field->{'flag'} >= 0)
            {
                push(@fields, $field->{'name'});
            }
        }
    }
    elsif ($table =~ /^ABMASTER(\d)+$/)
    {
        my (@schema);
        
        @fields = ("abID", 
                   "abSUBMITTER",
                   "abASSIGNEE",
                   &ISODateWrapper("abSUBMITDATE"),
                   &ISODateWrapper("abUPDATEDATE"),
                   "abSTATUS"
                  );

        @schema = &FP::getFieldsFromSchema("$ENV{'ABMASTER'}${FP::FS}MR${FP::FS}Schema");
        
        foreach my $field (@schema)
        {
            if ($field->{'type'} eq 'date' && $field->{'flag'} >= 0)
            {
                push(@fields, &ISODateWrapper($field->{'name'}));
            }
            elsif ($field->{'flag'} >= 0)
            {
                push(@fields, $field->{'name'});
            }
        }
    }
    elsif ($table =~ /^ABMASTER(\d)+\_MASTER$/)
    {
        my (@schema);
        
        @fields = ();

        @schema = &FP::getFieldsFromSchema("$ENV{'ABMASTER'}${FP::FS}MR${FP::FS}Schema");
        
        foreach my $field (@schema)
        {
            if ($field->{'type'} eq 'date' && $field->{'flag'} >= 0)
            {
                push(@fields, &ISODateWrapper($field->{'name'}));
            }
            elsif ($field->{'flag'} >= 0)
            {
                push(@fields, $field->{'name'});
            }
        }
    }
    elsif ($table =~ /_ABDATA/)
    {
        my (@schema);
        
        @fields = ("mrID");
        
        @schema = &FP::getFieldsFromSchema("$ENV{'ABMASTER'}${FP::FS}MR${FP::FS}Schema");
        
        foreach my $field (@schema)
        {
            if ($field->{'type'} eq 'date' && $field->{'flag'} >= 0)
            {
                push(@fields, &ISODateWrapper($field->{'name'}));
            }
            elsif ($field->{'flag'} >= 0)
            {
                push(@fields, $field->{'name'});
            }
        }
    }
    elsif ($table =~ /_HISTORY/)
    {
        @fields = ("mrID",
                   "mrGENERATION",
                   "mrHISTORY"
                  );
    }
    elsif ($table =~ /_TIMETRACKING/)
    {
        @fields = ("mrID",
                   "mrGENERATION",
                   "mrTIMESPENT",
                   "mrRATE",
                   &ISODateWrapper("mrTIMEDATE"),
                   "mrTIMEUSER",
                   "mrRATEDESC"
                  );
    }
    elsif ($table =~ /_DESCRIPTIONS/)
    {
        @fields = ("mrID",
                   "mrGENERATION",
                   "mrDESCRIPTION"
                  );
    }
    elsif ($table =~ /_APPROVALSTATES/)
    {
        @fields = ("mrID",
                   "mrPROCID",
                   "mrPHASEID",
                   "mrPHASESTATE",
                   &ISODateWrapper("mrSTATETIMEDATE"),
                   &ISODateWrapper("mrALERTTIMEDATE")
                  );
    }
    elsif ($table =~ /_APPROVALVOTES/)
    {
        @fields = ("mrID",
                  "mrPROCID",
                  "mrPHASEID",
                  "mrVOTEVALUE",
                  "mrVOTEUSER",
                  "mrVOTECOMMENT",
                  &ISODateWrapper("mrVOTETIMEDATE")
                  );
    }
    elsif ($table =~ /_APPROVALVOTESHIST/)
    {
        # (same as _APPROVALVOTES table)
        
        @fields = ("mrID",
                   "mrPROCID",
                   "mrPHASEID",
                   "mrVOTEVALUE",
                   "mrVOTEUSER",
                   "mrVOTECOMMENT",
                   &ISODateWrapper("mrVOTETIMEDATE")
                  );
    }
    elsif ($table =~ /_APPROVALPROCS/)
    {
        @fields = ("mrPROCID",
                   "mrORDER",
                   "mrPROCNAME",
                   "mrPROCDESC",
                   "mrPROCTRIGGER",
                   "mrPROCOPTIONS",
                   "mrDELETED"
                  );
    }
    elsif ($table =~ /_APPROVALPHASES/)
    {
        @fields = ("mrPROCID",
                   "mrPHASEID",
                   "mrORDER",
                   "mrPHASENAME",
                   "mrPHASEDESC",
                   "mrPHASEOPTIONS",
                   "mrDELETED",
                   "mrVOTEUSERS"
                  );
    }
    else
    {
        print "Unknown table: $table.\n";
        exit;
    }
    
    # We must verify that each field actually exists, and remove
    # ones that do not from the select clause.

    foreach my $field (@fields)
    {
        if (&FieldIsInTable($table, $field))
        {
            push (@finalFields, $field);
        }
        else
        {
            print "  $field not in $table - column will not be dumped.\n";
        }
    }
    
    $selectFields = join ',', @finalFields;

    return ($selectFields);    
}

#
# Name     : DumpCMMASTERToXML
# Synopsis : Dump tables relating to a CMMASTER to XML files 
# Arguments: None.
# Returns  : None.
# Notes    : Does not dump the ABMASTER table
#         
sub DumpCMMASTERToXML
{
    my ($xmlOut, $output, @projectTables, $baseTable, $abTable);
    
    $baseTable = &FP::getTableName($ENV{'CMMASTER'});
    
    $abTable = &FP::getTableName($ENV{'ABMASTER'});

    @projectTables = ("${baseTable}",
                      "${baseTable}_ABDATA",
                      "${baseTable}_HISTORY",
                      "${baseTable}_DESCRIPTIONS",
                      "${baseTable}_TIMETRACKING"
                     );
    
    # option to disable ABMASTER dump
    if (!$FP::noabmaster)
    {
        push (@projectTables, "$abTable");

        &checkForMasterRecordTable($ENV{'ABMASTER'});

        push (@projectTables, "${abTable}_MASTER");
    }
    
    # modified XML_RDB.pm to use FP::dbh and work with FootPrints
    $xmlOut = DBIx::FP_XML_RDB->new() || die "Failed to make new xmlOut.";
    
    # dump the project's issue tables and the address book
    
    foreach my $table (@projectTables)
    {
        my ($selectFields, $orderBy, $abMasterMaster);

        $abMasterMaster = "${abTable}_MASTER";

        if ($table eq $abTable)
        {
            $orderBy = "ORDER BY abID DESC";
        }
        elsif ($table !~ /$abMasterMaster/)
        {
            $orderBy = "ORDER BY mrID DESC";
        }
        else
        {
            $orderBy = "";
        }
        
        print "Dumping ${table} to ${FP::xmldir}${table}.xml\n\n";
        $selectFields = &FieldsInTable($table);
        $xmlOut->DoSql("SELECT $selectFields FROM $table $orderBy");   # dump newest issues first (older ones tend to have more problems loading)
        my $output = IO::File->new(">" . "${FP::xmldir}${table}.xml");
        print $output $xmlOut->GetData;
    }
    
    
    # dump the project's change management tables if they exist
    if (&FoundCMTables())
    {
        my (@cmTables);
        
        @cmTables = ("${baseTable}_APPROVALSTATES",
                     "${baseTable}_APPROVALVOTES",
                     "${baseTable}_APPROVALVOTESHIST",
                     "${baseTable}_APPROVALPROCS",
                     "${baseTable}_APPROVALPHASES");
        
        foreach my $table (@cmTables)
        {
            my ($selectFields);
            
            print "Dumping ${table} to ${FP::xmldir}${table}.xml\n\n";
            $selectFields = &FieldsInTable($table);
            $xmlOut->DoSql("SELECT $selectFields FROM $table");
            my $output = IO::File->new(">" . "${FP::xmldir}${table}.xml");
            print $output $xmlOut->GetData;
        } 
    }
    else
    {
        print "  Change Management tables do not exist, they will not be dumped.";
    }
}


#
# Name     : FieldIsInTable
# Synopsis : Verifies that a given field is in a table by selecting it
# Arguments: table, field
# Returns  : Returns 1 if exists, 0 otherwise.
# Notes    : None.
#     
sub FieldIsInTable
{
    my (%restore, $table, $field);
        
    $table = $_[0];
    $field = $_[1];

    $restore{'RaiseError'} = $FP::dbh->{RaiseError};
    $restore{'PrintError'} = $FP::dbh->{PrintError};
    
    $FP::dbh->{RaiseError} = 1;
    $FP::dbh->{PrintError} = 0;
    
    eval
    {
        # test for existance of field
        $FP::dbh->do("select $field FROM $table");
    };
    
    $FP::dbh->{RaiseError} = $restore{'RaiseError'};
    $FP::dbh->{PrintError} = $restore{'PrintError'};
    
    if ($@)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

#
# Name     : FoundCMTables
# Synopsis : Tests for existance of Change Management tables
# Arguments: None.
# Returns  : Returns 1 if they exist, 0 otherwise.
# Notes    : None.
#       
sub FoundCMTables
{
    my (%restore, $baseTable);

    $baseTable = &FP::getTableName($ENV{'CMMASTER'});
    
    $restore{'RaiseError'} = $FP::dbh->{RaiseError};
    $restore{'PrintError'} = $FP::dbh->{PrintError};

    $FP::dbh->{RaiseError} = 1;
    $FP::dbh->{PrintError} = 0;
    
    eval
    {
        # test for existance of change management tables
        $FP::dbh->do("select 1 FROM ${baseTable}_APPROVALSTATES");
    };
 
    $FP::dbh->{RaiseError} = $restore{'RaiseError'};
    $FP::dbh->{PrintError} = $restore{'PrintError'};

    if ($@)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

####################################################################
## :main: 
####################################################################

if (!$ENV{'CMMASTER'})
{
    print "CMMASTER environment is not set, cannot continue.\n";
    exit;
}

if (!$ENV{'ABMASTER'})
{
    print "ABMASTER environment is not set, cannot continue.\n";
    exit;
}

if (!(-d $ENV{'CMMASTER'}))
{
    print "CMMASTER directory of $ENV{'CMMASTER'} does not exist, cannot continue.\n";
    exit;
}

if (!(-d $ENV{'ABMASTER'}))
{
    print "ABMASTER directory of $ENV{'ABMASTER'} does not exist, cannot continue.\n";
    exit;
}

&FP::connectToDBD;

# options variables, global to FP package
local our $mrids_file = '';       # file of IDs to dump
local our $noabmaster = '';       # do not dump the ABMASTER table
local our $debug = '';            # enable debugging
local our $help = '';             # print help
local our $xmldir = "$ENV{'CMMASTER'}${FP::FS}MR${FP::FS}";

&GetOptions('mrids'      =>\$mrids_file,
            'noabmaster' =>\$noabmaster,
            'debug'       =>\$debug,
            'help'        =>\$help,
            'xmldir=s'      =>\$xmldir
            ''
           );

if ($help)
{
    print "\n";
    print "valid arguments: \n";
    print "-mrids [file]     file of IDs to dump, one per line\n";
    print "-noabmaster       do not dump the ABMASTER table\n";
    print "-debug            display debug information\n";
    print "-xmldir           path to save xml files to\n";
    print "-help             show this help\n";
    print "\n";
    exit;
}

&DumpCMMASTERToXML();

exit;



