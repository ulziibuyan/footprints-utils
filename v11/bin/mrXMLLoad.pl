#!/usr/footprints_perl/bin/perl --
#
# Copyright 1996-2012 Numara Software Inc.                #COPYRIGHT LINE#
#

use strict;
package XMLDBI;

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

use IO::File;
use DBI qw/:sql_types/;
use XML::Parser;
use MIME::Base64;
use MRPasswdTranslation;
use MRUsersTranslation;
use TicketRelationship::DBStructure;
use FPGenericSequence;

local our %attrs;
local our %autoInc;

FP::connectToDBD();

our (@ISA, $table, $sth, %colVal, @colNames, $lastSQL, $lastTable, $recordCount);

@ISA= ("XML::Parser");

#
# Name     : IsDateField
# Synopsis : Returns 1 or 0 if field is a date field
# Arguments: COLUMN => column, 
#            TABLE => table (optional)
# Returns  : None.
# Notes    : None.
#        
sub IsDateField
{
    my ($col, $thisTable, @schema, %dateFields, %args);
    
    %args = @_;

    $col = uc($args{'COLUMN'});
    $thisTable = uc($args{'TABLE'});

    # if loading the users, we know which fields are date fields and
    # the static table schema that we are loading.

    if ($main::users)
    {
        my @usersTableFields = @ { $MRPasswdTranslation::USERS_TABLE_SCHEMA->{'fields'} };

         foreach my $field (@usersTableFields)
         {
             if ($field->{'type'} eq 'DATETIME')
             {
                 my $fieldName = uc($field->{'name'});
                 $dateFields{$fieldName} = 1;
             }
         }

         if ($dateFields{$col})
         {
             return 1;
         }
         else
         {
             return 0;
         }
     }

    # if loading the user profiles, we know which fields are date fields and
    # the static table schema that we are loading.

    if ($main::user_profiles)
    {
        my @userProfilesTableFields = @ { $MRUsersTranslation::USER_PROFILES_TABLE_SCHEMA->{fields} };

         foreach my $field (@userProfilesTableFields)
         {
             if ($field->{'type'} eq 'DATETIME')
             {
                 my $fieldName = uc($field->{'name'});
                 $dateFields{$fieldName} = 1;
             }
         }

         if ($dateFields{$col})
         {
             return 1;
         }
         else
         {
             return 0;
         }
     }

    
    # if loading the calendar, we know which fields are date fields and
    # the static table schemas that we are loading.
    
    if ($main::cal)
    {
        $dateFields{'STIME'} = 1;
        $dateFields{'ETIME'} = 1;
        $dateFields{'CREATESTAMP'} = 1;
        $dateFields{'EDITSTAMP'} = 1;
        $dateFields{'RECUREND'} = 1;
        
        if ($dateFields{$col})
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
   
    if ($main::cmdbAll)
    {
        if ($thisTable =~ /^CMDB(\d)+_HISTORY$/)
        {
            $dateFields{'ACTION_TIME'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_RELATIONS$/ || $thisTable =~ /^CMDB(\d)+\_RELATIONS\_GENERATIONS$/)
        {
            $dateFields{'SUBMIT_SYSTIMESTAMP'} = 1;
            $dateFields{'REVISION_SYSTIMESTAMP'} = 1;
            $dateFields{'SUBMIT_USERTIMESTAMP'} = 1;
            $dateFields{'REVISION_USERTIMESTAMP'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_CI\_(\d)+$/ || $thisTable =~ /^CMDB(\d)+\_CI\_(\d)+\_GENERATIONS$/)
        {
            my $cmdbId = $1;
            my $ciTypeId= $2;

            $dateFields{'SUBMIT_SYSTIMESTAMP'} = 1;
            $dateFields{'REVISION_SYSTIMESTAMP'} = 1;
            $dateFields{'SUBMIT_USERTIMESTAMP'} = 1;
            $dateFields{'REVISION_USERTIMESTAMP'} = 1;
            $dateFields{'LASTEDIT_SYSTIMESTAMP'} = 1;
            $dateFields{'LASTEDIT_USERTIMESTAMP'} = 1;
        
            my @CIFields = CMDB::CIFields->getCIFields('CMDB_ID' => $cmdbId,
                                                       'CI_TYPE_ID' => $ciTypeId);

            foreach my $field (@CIFields)
            {
                my $fieldType = $field->getCIFieldType();
                my $columnName = $field->getCIColumnName();
            
                if ($fieldType eq 'date')
                {
                    $dateFields{$columnName} = 1;
                }
            }
        }
        
        if ($dateFields{$col})
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
    
    if($thisTable eq 'FBLAYOUT')
    {
        return (($col eq 'LOCKEDDATE') || ($col eq 'LASTMODIFIEDDATE'));
    }
   
    # used by FP_LOGINS
    $dateFields{'LAST_ACTIVE'} = 1;
    $dateFields{'LAST_BROWSER_CHECK'} = 1;

    $dateFields{'MRUPDATEDATE'} = 1;
    $dateFields{'MRSUBMITDATE'} = 1;
    $dateFields{'ABSUBMITDATE'} = 1;
    $dateFields{'ABUPDATEDATE'} = 1;
    $dateFields{'MRTIMEDATE'} = 1;
    $dateFields{'MRSTATETIMEDATE'} = 1;
    $dateFields{'MRALERTTIMEDATE'} = 1;
    $dateFields{'MRVOTETIMEDATE'} = 1;
    
    if ($thisTable =~ /_FIELDHISTORY/ || $thisTable =~ /_KBVOTE/ || $thisTable eq 'FP_licenseUse' || $thisTable eq 'FPGenericConfigTable' || $thisTable =~ /_STATUS/)
    {        
        # careful that mrTIMESTAMP in other tables is not a date (ms sql)
        $dateFields{'MRTIMESTAMP'} = 1;
    }
    
    foreach my $field (@{$args{'PROJSCHEMA'}})
    {
        if ($field->{'type'} eq 'date' && $field->{'flag'} >= 0)
        {
            my ($name);
            
            $name = uc($field->{'name'});
            
            $dateFields{$name} = 1;
        }
    }
    
    foreach my $field (@{$args{'ABSCHEMA'}})
    {
        if ($field->{'type'} eq 'date' && $field->{'flag'} >= 0)
        {
            my ($name);
            
            $name = uc($field->{'name'});
            
            $dateFields{$name} = 1;
        }
    }
    
    if ($dateFields{$col})
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

#
# Name     : IsLongTextField
# Synopsis : Returns 1 or 0 if field is a long text field
# Arguments: COLUMN => column, 
#            TABLE => table (optional)            
# Returns  : None.
# Notes    : None.
#  
sub IsLongTextField
{
    my ($col, $thisTable, @schema, %longFields, %args);
    
    %args = @_;

    $col = uc($args{'COLUMN'});
    $thisTable = uc($args{'TABLE'});

    # if loading the users, we know which fields are date fields and
    # the static table schema that we are loading.

    if ($main::users)
    {
        my @usersTableFields = @ { $MRPasswdTranslation::USERS_TABLE_SCHEMA->{'fields'} };

         foreach my $field (@usersTableFields)
         {
             if ($field->{'type'} eq 'CHARMULTI')
             {
                 my $fieldName = uc($field->{'name'});
                 $longFields{$fieldName} = 1;
             }
         }

         if ($longFields{$col})
         {
             return 1;
         }
         else
         {
             return 0;
         }
     }

    # if loading the user profiles, we know which fields are date fields and
    # the static table schema that we are loading.

    if ($main::users)
    {
        my @userProfilesTableFields = @ { $MRUsersTranslation::USER_PROFILES_TABLE_SCHEMA->{'fields'} };

         foreach my $field (@userProfilesTableFields)
         {
             if ($field->{'type'} eq 'CHARMULTI')
             {
                 my $fieldName = uc($field->{'name'});
                 $longFields{$fieldName} = 1;
             }
         }

         if ($longFields{$col})
         {
             return 1;
         }
         else
         {
             return 0;
         }
     }


    # if loading the calendar, we know which fields are date fields and
    # the static table schema that we are loading.

    if ($main::cal)
    {
        $longFields{'DESCRIPTION'} = 1;

        if ($longFields{$col})
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    if ($main::cmdbAll)
    {
        if ($thisTable =~ /^CMDB_EXTENDEDPREFS$/)
        {
            $longFields{'PREF_VALUE'} = 1;
        }
        elsif ($thisTable =~ /^CMDB_USERPREFS$/)
        {
            $longFields{'PREF_VALUE'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_ESCALATIONS$/)
        {
            $longFields{'EMAIL_ATTR'} = 1;
            $longFields{'EMAIL_TMPL_BODY'} = 1;
            $longFields{'EMAIL_ADDL'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_HISTORY$/)
        {
            $longFields{'FROMVALUE'} = 1;
            $longFields{'TOVALUE'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_IMPORT_RULESETS$/)
        {
            $longFields{'RULESET_CONFIG'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_ROLE_PROPERTIES$/)
        {
            $longFields{'PROPERTY_LONG'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_SAVED_REPORTS$/)
        {
            $longFields{'REPORT_PARAMS'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_SAVED_SEARCHES$/)
        {
            $longFields{'SEARCH_DESCRIPTION'} = 1;
            $longFields{'SEARCH_CIS'} = 1;
            $longFields{'SEARCH_FILTER'} = 1;
            $longFields{'SEARCH_RELATED_CIS_FROM'} = 1;
            $longFields{'SEARCH_RELATED_CIS_TO'} = 1;
            $longFields{'SEARCH_RELATED_CIS_FILTER'} = 1;
            $longFields{'SEARCH_ATTRIBUTES'} = 1;
        }
        elsif ($thisTable =~ /^CMDB(\d)+\_CI\_(\d)+$/ || $thisTable =~ /^CMDB(\d)+\_CI\_(\d)+\_GENERATIONS$/)
        {
            my $cmdbId = $1;
            my $ciTypeId= $2;
            
            my @CIFields = CMDB::CIFields->getCIFields('CMDB_ID' => $cmdbId,
                                                       'CI_TYPE_ID' => $ciTypeId);
            
            foreach my $field (@CIFields)
            {
                my $fieldType = $field->getCIFieldType();
                my $columnName = $field->getCIColumnName();
                
                if ($fieldType eq 'multiC')
                {
                    $longFields{$columnName} = 1;
                }
            }
        }

        if ($longFields{$col})
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
    if($thisTable eq 'FBLAYOUT')
    {
        return ($col eq 'LAYOUT');
    }
   
    
    $longFields{'MRDESCRIPTION'} = 1;
    $longFields{'MRALLDESCRIPTIONS'} = 1;
    $longFields{'MRVOTECOMMENT'} = 1;
    $longFields{'MRPROCDESC'} = 1;
    $longFields{'MRPROCTRIGGER'} = 1;
    $longFields{'MRPROCOPTIONS'} = 1;
    $longFields{'MRPHASEDESC'} = 1;
    $longFields{'MRPHASEOPTIONS'} = 1;
    
    if ($thisTable =~ /_KBVOTE/)
    {        
        # careful that mrCOMMENT in MASTERx is varchar
        $longFields{'MRCOMMENT'} = 1;
    }
    
    foreach my $field (@{$args{'PROJSCHEMA'}})
    {
        if ($field->{'type'} eq 'char' && $field->{'multi'} eq 'multi' && $field->{'flag'} >= 0)
        {
            my ($name);
            
            $name = uc($field->{'name'});
            
            $longFields{$name} = 1;
        }
    }
    
    foreach my $field (@{$args{'ABSCHEMA'}})
    {
        if ($field->{'type'} eq 'char' && $field->{'multi'} eq 'multi' && $field->{'flag'} >= 0)
        {
            my ($name);
            
            $name = uc($field->{'name'});
            
            $longFields{$name} = 1;
        }
    }
    
    if ($longFields{$col})
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

#
# Name     : IsAutoIncrement
# Synopsis : Returns 1 or 0 if field is an auto increment field and changed VALUE to the new sequence value
# Arguments: COLUMN => column, 
#            TABLE => table
#            VALUE => ref to field value
# Returns  : None.
# Notes    : Supporting tables: MASTER_RELATIONSHIPMEMBER
#  
sub IsAutoIncrement
{
    my ($col, $thisTable, $refValue, %args);

    %args = @_;
    $col = uc($args{'COLUMN'});
    $thisTable = uc($args{'TABLE'});
    $refValue = $args{'VALUE'};
    # Try to get from cache
    if (exists($autoInc{$thisTable}) && exists($autoInc{$thisTable}->{$col}) && exists($autoInc{$thisTable}->{$col}->{${$refValue}}))
    {
        ${$refValue} = $autoInc{$thisTable}->{$col}->{${$refValue}};
        return 1;
    }
    # Generate new
    if (($thisTable eq 'MASTER_RELATIONSHIPMEMBER') && ($col eq 'MASTER_RELATIONSHIPMEMBERID'))
    {
        my $sequence = FPGenericSequence->new({ dbh => $FP::dbh });
        my $seqId = $sequence->Next(&TicketRelationship::DBStructure::REL_TABLE_SEQ_KEY);
        $autoInc{$thisTable}->{$col}->{${$refValue}} = $seqId;
        ${$refValue} = $seqId;
        return 1;
    }
    elsif (($thisTable eq 'MASTER_RELATIONSHIPMEMBER') && ($col eq 'RELATIONSHIPID'))
    {
        my $sequence = FPGenericSequence->new({ dbh => $FP::dbh });
        my $relId = $sequence->Next(&TicketRelationship::DBStructure::REL_TABLE_REL_KEY);
        $autoInc{$thisTable}->{$col}->{${$refValue}} = $relId;
        ${$refValue} = $relId;
        return 1;
    }
    else
    {
        return 0;
    }
}

# Name     : FindLoadTables
# Synopsis : Sets @main::loadTables with tables to be loaded from xml
# Arguments: None.
# Returns  : None.
# Notes    : None.
#  
sub FindLoadTables
{
    my ($baseTable, $abTable);

    if ($main::cmdbAll)
    {
        my ($dh);

        opendir ($dh, $main::xmldir);
        my @files = grep(/^CMDB(.*?)\.xml$/i, readdir $dh);
        closedir $dh;

        foreach my $file (@files)
        {
            my $table = $file;
            
            $table =~ s/\.xml//i;

            # Exceptions for CMDB_MAIN and CMDB_PROJECTS
            if (($table ne 'CMDB_MAIN') && ($table ne 'CMDB_PROJECTS'))
            {
                push (@main::loadTables, "$table");
            }
        }

        return;
    }
    
    # load the CMDB common tables only
    if ($main::cmdbCommon)
    {
        push(@main::loadTables, "CMDB_MAIN") if -e "${main::xmldir}CMDB_MAIN.xml";
        push(@main::loadTables, "CMDB_PROJECTS") if -e "${main::xmldir}CMDB_PROJECTS.xml";
        return;
    }

    # dump generic config table only
    if ($main::generic)
    {
        push (@main::loadTables, "FPGenericConfigTable") if -e "${main::xmldir}FPGenericConfigTable.xml";
        push (@main::loadTables, "FPGenericSequence_state") if -e "${main::xmldir}FPGenericSequence_state.xml";
        push (@main::loadTables, "FPGenericSequence_release") if -e "${main::xmldir}FPGenericSequence_release.xml";
        push (@main::loadTables, "FIELDHISTORY_state_table") if -e "${main::xmldir}FIELDHISTORY_state_table.xml";
        push (@main::loadTables, "FIELDHISTORY_release_table") if -e "${main::xmldir}FIELDHISTORY_release_table.xml";
        return;
    }

    # dump logins table only
    if ($main::logins)
    {
        push (@main::loadTables, "FP_LOGINS") if -e "${main::xmldir}FP_LOGINS.xml";
        return;
    }

    # dump user table only
    if ($main::users)
    {
        push (@main::loadTables, "$MRPasswdTranslation::USERS_TABLE_NAME") if -e "${main::xmldir}${MRPasswdTranslation::USERS_TABLE_NAME}.xml";
        return;
    }

    # dump user_profiles table only
    if ($main::user_profiles)
    {
        push (@main::loadTables, "$MRUsersTranslation::USER_PROFILES_TABLE_NAME") if -e "${main::xmldir}${MRUsersTranslation::USER_PROFILES_TABLE_NAME}.xml";
        return;
    }

    # dump the license use tables only
    if ($main::licenseuse)
    {
        push (@main::loadTables, "FP_licenseUse") if -e "${main::xmldir}FP_licenseUse.xml";
        return;
    }
    
    # dump the FP_NAMP tables only
    if ($main::fp_namp)
    {
        push (@main::loadTables, "FP_NAMP") if -e "${main::xmldir}FP_NAMP.xml";
        return;
    }

    # load the FullTextExcludeCols table only
    if ($main::fultextcols)
    {
        push (@main::loadTables, "FulltextExcludeCols") if -e "${main::xmldir}FulltextExcludeCols.xml";
        return;
    }

    # load the MASTER_RelationshipMember table only
    if (defined($main::relationship))
    {
        my $filename = "MASTER_RelationshipMember" . $main::relationship;
        push (@main::loadTables, "MASTER_RelationshipMember") if -e "${main::xmldir}$filename.xml";
        if (-e "${main::xmldir}${filename}crossWSrelationships.xml" )
        {
            push (@main::loadTables, "MASTER_RelationshipMember");
            $filename = $filename . "crossWSrelationships";
        }
        $main::loadFiles{"MASTER_RelationshipMember"} = $filename;
        return;
    }

    # load the FBLAYOUT table only
    if($main::fblayout)
    {
        my $filename = "FBLayout" . $main::fblayout;
        push (@main::loadTables, "FBLayout") if -e "${main::xmldir}${filename}.xml";
        $main::loadFiles{"FBLayout"} = $filename;
        return;
    }   

    # load the SMConnectors table only
    if($main::smconnectors)
    {
        my $filename = "SMConnectors";
        push (@main::loadTables, "SMConnectors") if -e "${main::xmldir}${filename}.xml";
        $main::loadFiles{"SMConnectors"} = $filename;
        return;
    }   

    # load the FPPWS tables only
    if ($main::fppws)
    {
        if (-e "${main::xmldir}FPPWS_LOGIN_HISTORY.xml")
        {
            push (@main::loadTables, "FPPWS_LOGIN_HISTORY");
        }
        if (-e "${main::xmldir}FPPWS_PASSWORD_HISTORY.xml")
        {
            push (@main::loadTables, "FPPWS_PASSWORD_HISTORY");
        }
        if (-e "${main::xmldir}FPPWS_Sequence_release.xml")
        {
            push (@main::loadTables, "FPPWS_Sequence_release");
        }
        if (-e "${main::xmldir}FPPWS_Sequence_state.xml")
        {
            push (@main::loadTables, "FPPWS_Sequence_state");
        }
        return;
    }

    # load calendar tables only
    if ($main::cal)
    {
        push (@main::loadTables, "FPCalMain") if -e "${main::xmldir}FPCalMain.xml";
        push (@main::loadTables, "FPCalTicketLink") if -e "${main::xmldir}FPCalTicketLink.xml";
        push (@main::loadTables, "FPCalPrefs") if -e "${main::xmldir}FPCalPrefs.xml";
        push (@main::loadTables, "FPCal_state_table") if -e "${main::xmldir}FPCal_state_table.xml";
        push (@main::loadTables, "FPCal_release_table") if -e "${main::xmldir}FPCal_release_table.xml";
        return;
    }
    
    # standard project / ab dump

    $baseTable = &FP::getTableName($ENV{'CMMASTER'});
    
    $abTable = &FP::getTableName($ENV{'ABMASTER'});
    
    if (-e "${main::xmldir}${baseTable}.xml")
    {
        push (@main::loadTables, "${baseTable}") ;
        push @{$main::relatedTables{$baseTable}}, "${baseTable}_ASSIGNMENT";    #MASTER!X!_ASSIGNMENT table has forign key connected with MASTER!X!, so we have to truncate this table first
        push (@main::loadTables, "${baseTable}_ASSIGNMENT") if -e "${main::xmldir}${baseTable}_ASSIGNMENT.xml";
        push @{$main::relatedTables{$baseTable}}, "${baseTable}_SMC";    #MASTER!X!_SMC table has forign key connected with MASTER!X!, so we have to truncate this table first
        push (@main::loadTables, "${baseTable}_SMC") if -f "${main::xmldir}${baseTable}_SMC.xml";
        push @{$main::relatedTables{$baseTable}}, "${baseTable}_TwitterIssues";    #MASTER!X!_TwitterIssues table has forign key connected with MASTER!X!, so we have to truncate this table first
        push (@main::loadTables, "${baseTable}_TwitterIssues") if -f "${main::xmldir}${baseTable}_TwitterIssues.xml";
    }
        
    push (@main::loadTables, "${baseTable}_ABDATA") if -e "${main::xmldir}${baseTable}_ABDATA.xml";
    push (@main::loadTables, "${baseTable}_HISTORY") if -e "${main::xmldir}${baseTable}_HISTORY.xml";
    push (@main::loadTables, "${baseTable}_DESCRIPTIONS") if -e "${main::xmldir}${baseTable}_DESCRIPTIONS.xml";
    push (@main::loadTables, "${baseTable}_TIMETRACKING") if -e "${main::xmldir}${baseTable}_TIMETRACKING.xml";
    push (@main::loadTables, "${baseTable}_NAMPORs") if -e "${main::xmldir}${baseTable}_NAMPORs.xml";
    
    push (@main::loadTables, "${baseTable}_KBVOTE") if -e "${main::xmldir}${baseTable}_KBVOTE.xml";
    push (@main::loadTables, "${baseTable}_FIELDHISTORY") if -e "${main::xmldir}${baseTable}_FIELDHISTORY.xml";
    push (@main::loadTables, "${baseTable}_STATUS") if -e "${main::xmldir}${baseTable}_STATUS.xml";

    push (@main::loadTables, "${baseTable}_IssueSummaryStateDim") if -f "${main::xmldir}${baseTable}_IssueSummaryStateDim.xml";
    push (@main::loadTables, "${baseTable}_ISSUESTATUS") if -f "${main::xmldir}${baseTable}_ISSUESTATUS.xml";

    push (@main::loadTables, "SMConnectors") if -f "${main::xmldir}SMConnectors.xml";
    
    # option to not load the abmaster table
    if (!$main::noabmaster)
    {
        push (@main::loadTables, "$abTable") if -e "${main::xmldir}${abTable}.xml";
        push (@main::loadTables, "${abTable}_MASTER") if -e "${main::xmldir}${abTable}_MASTER.xml";
    }
    
    push (@main::loadTables, "${baseTable}_APPROVALSTATES") if -e "${main::xmldir}${baseTable}_APPROVALSTATES.xml";
    push (@main::loadTables,"${baseTable}_APPROVALVOTES") if -e "${main::xmldir}${baseTable}_APPROVALVOTES.xml";
    push (@main::loadTables,"${baseTable}_APPROVALVOTESHIST") if -e "${main::xmldir}${baseTable}_APPROVALVOTESHIST.xml";
    push (@main::loadTables,"${baseTable}_APPROVALPROCS") if -e "${main::xmldir}${baseTable}_APPROVALPROCS.xml";
    push (@main::loadTables,"${baseTable}_APPROVALPHASES") if -e "${main::xmldir}${baseTable}_APPROVALPHASES.xml";

    if($ENV{'CMMASTER'} =~ /MASTER(\d+)/)
    {
        my $WSID = $1;
        my $filename = "FBLayout" . $WSID;
        push (@main::loadTables, "FBLayout") if -e "${main::xmldir}${filename}.xml";
        $main::loadFiles{"FBLayout"} = $filename;

        $filename = "FPTransactionData" . $WSID;
        push (@main::loadTables, "FPTransactionData") if -e "${main::xmldir}${filename}.xml";    
        $main::loadFiles{"FPTransactionData"} = $filename;
        
        $filename = "FPNAMPOR_state_table" . $WSID;
        push (@main::loadTables, "FPNAMPOR_state_table") if -e "${main::xmldir}${filename}.xml";
        $main::loadFiles{"FPNAMPOR_state_table"} = $filename;
        
        $filename = "FPNAMPOR_release_table" . $WSID;
        push (@main::loadTables, "FPNAMPOR_release_table") if -e "${main::xmldir}${filename}.xml";
        $main::loadFiles{"FPNAMPOR_release_table"} = $filename;
        
        my $filename = "MASTER_RelationshipMember" . $WSID;
        push (@main::loadTables, "MASTER_RelationshipMember") if -e "${main::xmldir}${filename}.xml";
        $main::loadFiles{"MASTER_RelationshipMember"} = $filename;
    }
}
   
#
# GetSqlTableName has been removed, because it's the fix has been duplicated by the fix for LEO #26334, r21720

#
# Name     : new
# Synopsis : 'new' event handler for XML Load
# Arguments: None.
# Returns  : None.
# Notes    : None.
#     
sub new 
{
    my($proto) = shift @_;
    my($class) = ref($proto) || $proto;
    my($self) = $class->SUPER::new(@_);
    
    $table = shift; 
    
    bless($self, $class);
    
    $self->setHandlers('Start' => $self->can('Start'),
                       'End'  => $self->can('End'),
                       'Char' => $self->can('Char'),
                       'Init' => $self->can('Init'));
    
    return($self);
}

#
# Name     : Init
# Synopsis : 'Init' event handler for XML Load
# Arguments: None.
# Returns  : None.
# Notes    : None.
#     
sub Init
{
    $FP::dbh->{AutoCommit} = 0;
    $FP::dbh->{RaiseError} = 1;
    
}

#
# Name     : Start
# Synopsis : 'Start' event handler for XML Load
# Arguments: None.
# Returns  : None.
# Notes    : None.
#     
sub Start 
{
    my ($expat, $element);
    ($expat, $element, %attrs) = @_;

    if ($expat->within_element("ROW") || $expat->within_element("row")) 
    {
        # OK, got a column, reset the data within that column
        undef $expat->{ __PACKAGE__ . "currentData"};
    }
}

#
# Name     : End
# Synopsis : 'End' event handler for XML Load
# Arguments: None.
# Returns  : None.
# Notes    : None.
#     
{
    local our (@projSchema, @abSchema);
sub End 
{
    my ($expat, $element) = @_;
    
    unless(@projSchema && @abSchema)
    {
        @projSchema = &FP::getFieldsFromSchema("$ENV{'CMMASTER'}${FP::FS}MR${FP::FS}Schema");
        @abSchema = &FP::getFieldsFromSchema("$ENV{'ABMASTER'}${FP::FS}MR${FP::FS}Schema");
    }

    if ($element =~ m/^ROW$/i) 
    {    
        my ($columnString, $bindString, $sql, $position);
        my (@paramList);
		
		#We should ignore the value of column mrTIMESTAMP in some tables in SQL-SERVER based systems, 
		#otherwise we may run into database error
		if ($table=~/^MASTER\d+$|^MASTER\d+_ABDATA$|^ABMASTER\d+$|^ABMASTER\d+_MASTER$/i){
            @colNames = grep { !/MRTIMESTAMP/i } @colNames;
		}
		
        if ($table =~ /^MASTER\d+$/)
        {
            # Remove old columns before dump loading
            my @excludedColumns = qw(mrREF_TO_MRX mrREF_TO_MR);
            my @newColNames = qw();
            
            foreach my $item (@colNames)
            {
                push (@newColNames, $item) if (!grep(m/$item/i, @excludedColumns));
            }
            
            @colNames = @newColNames;
        }
        $columnString = join ",", @colNames;

        foreach my $col (@colNames)
        {
            # bad test to see if it is a date field
            if (IsDateField('COLUMN' => $col, 
                            'TABLE' => $table,
                            'PROJSCHEMA' => \@projSchema,
                            'ABSCHEMA' => \@abSchema))
            {
                my ($toDate);

                # dates should never end in .657 for example, so strip it off
                $colVal{$col} =~ s/\.\d+$//;

                if ($colVal{$col} =~ /0000/)
                {
                    push (@paramList, "NULL");
                } 
                elsif ($FP::POSTGRES_DBMS)
                {
                    push (@paramList, "TO_TIMESTAMP('$colVal{$col}', 'YYYY-MM-DD HH24:MI:SS')"); 
                }
                elsif ($FP::ORACLE_DBMS)
                {
                    push (@paramList, "TO_DATE('$colVal{$col}', 'YYYY-MM-DD HH24:MI:SS')"); 
                }
                elsif ($FP::SQL_DBMS)
                {
                    my ($dateStripped);

                    $dateStripped = "'$colVal{$col}'";
                    $dateStripped =~ s/\-//g;

                    push(@paramList, $dateStripped);
                }
                else
                {
                    push (@paramList, "'$colVal{$col}'");
                }
            }
            else
            {
                push (@paramList, "?");
            }
        }
        
        $bindString = join ', ', @paramList;

        eval {
            $sql = "INSERT INTO $table ($columnString) VALUES ($bindString)";

            if ($sql ne $lastSQL)
            {
               # If this SQL is different from the last query we executed, it is
               # time to prepare a new query.
               $sth = $FP::dbh->prepare($sql);               
               
               $lastSQL = $sql;
            }

            foreach my $col (@colNames)
            {
                # skip date fields, they are not bound params
                if (IsDateField('COLUMN' => $col, 
                                'TABLE' => $table,
                                'PROJSCHEMA' => \@projSchema,
                                'ABSCHEMA' => \@abSchema))
                {
                    next;
                }

                if (uc($col) eq 'MRSTATUS' || uc($col) eq 'MRSUBMITTER' || uc($col) eq 'ABSUBMITTER')
                {
                    # trim space from char(254) mrSTATUS values
                    $colVal{$col} =~ s/\s*$//;
                }

                # convert encoding if necessary
                use Encode qw();
                if (!&FP::Utf8Enabled())
                {
                    my $encoding = defined $FP::LOCAL_ENCODING ? $FP::LOCAL_ENCODING : 'iso-8859-1';
                    $colVal{$col} = Encode::decode_utf8($colVal{$col});
                    $colVal{$col} = Encode::encode($encoding, $colVal{$col});
                }
                
                # handle LONG_VARCHAR where necessary
                
                if (&IsLongTextField('COLUMN' => $col, 
                                     'TABLE' => $table,
                                     'PROJSCHEMA' => \@projSchema,
                                     'ABSCHEMA' => \@abSchema))
                {
                    $sth->bind_param(++$position, $colVal{$col}, DBI::SQL_LONGVARCHAR); 
                }
                elsif (&IsAutoIncrement('COLUMN' => $col,
                                        'TABLE' => $table,
                                        'VALUE' => \$colVal{$col}))
                {
                    $sth->bind_param(++$position, $colVal{$col});
                }
                else
                {
                    if ($col ne 'MRASSIGNEES' && $col ne 'MRATTACHMENTS'
                        && $col ne 'MRREF_TO_MRX')
                    {
                        # truncate any varchar(254) fields that are too large
                        $colVal{$col} = substr($colVal{$col},0,253);
                    }
                    
                    $sth->bind_param(++$position, $colVal{$col}); 
                }
            }
            
            $sth->execute();
            
            if ($lastTable ne $table)
            {
                $recordCount = 0;
                $lastTable = $table;
            }
            
            $recordCount++;
            if (($recordCount % 1000) == 0)
            {
                print "Inserted record $recordCount into table '$table'\n";
            }

            $FP::dbh->commit();
        };

        if ($@)
        {
            print "$@\n";
            exit;
        }
        
        print "$sql\n" if $main::debug;
           
        # re-initialize for next record
        undef %colVal;
        undef @colNames;
    }
    elsif ($expat->within_element("ROW") || $expat->within_element("row")) 
    {
        $element = uc($element);
        
        $colVal{$element} = 
            $expat->{ __PACKAGE__. "currentData"};
            
        if ($attrs{'dbi:encoding'} eq 'base64')
        {
            $colVal{$element} = MIME::Base64::decode($colVal{$element});
        }
        
        push (@colNames, "$element");
    }
}
}

#
# Name     : Char
# Synopsis : 'Char' event handler for XML Load
# Arguments: None.
# Returns  : None.
# Notes    : None.
#     
sub Char 
{
    my ($expat, $string) = @_;
    my @context = $expat->context;
    my $column = pop @context;
    my $curtable = pop @context;
    
    if (($curtable) && ($curtable =~ m/^ROW$/i)) 
    {
        $expat->{ __PACKAGE__ . "currentData"} .= $string;
    }
}

#
# Name     : ArgAndEnvChecks
# Synopsis : Exits on bad args or environment settings
# Arguments: None.
# Returns  : None.
# Notes    : None.
# 
sub ArgAndEnvChecks
{
    # cmmaster does not need to be set for these tables
    if (!$main::cal && !$main::users && !$main::user_profiles && !$main::licenseuse &&
        !$main::logins && !$main::generic && !$main::cmdbAll && !$main::cmdbCommon &&
        !$main::fp_namp && !$main::fblayout && !$main::fppws && !$main::fultextcols &&
        !defined($main::relationship) && !$main::smconnectors)
    {
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
    }
}

1;
#########################################################################################

package main;

use strict;

our ($table, $inputfile, @loadTables, %loadFiles, %relatedTables, %alreadyTruncated);

# options variables, global to main package
local our $noabmaster;              # do not dump the ABMASTER table
local our $debug;                   # enable debugging
local our $help;                    # print help
local our $truncate;                # truncate tables prior to load
local our $xmldir;                  # xml dir
local our $cal;                     # only load calendar tables
local our $users;                   # only load user table
local our $user_profiles;           # only load user_profiles table
local our $licenseuse;              # only load license use table
local our $logins;                  # only load logins table
local our $generic;                 # only load generic config table
local our $cmdbAll;                 # load all cmdb files from existing xml except the common ones
local our $cmdbCommon;              # only load CMDB_MAIN and CMDB_PROJECTS
local our $fp_namp;                 # only load FP_NAMP
local our $fppws;                   # only load FPPWS
local our $fblayout;                # only load FBLAYOUT
local our $fultextcols;             # only load FullTextExcludeCols
local our $relationship;            # only load MASTER_RelationshipMember
local our $smconnectors;            # only load SMConnectors
local our $keepshared;              # doesn't delete shared tables

use Getopt::Long;  # this use must be here

&GetOptions('noabmaster'        => \$noabmaster,
            'debug'             => \$debug,
            'truncate'          => \$truncate,
            'help'              => \$help,
            'xmldir=s'          => \$xmldir,
            'cal'               => \$cal,
            'users'             => \$users,
            'user_profiles'     => \$user_profiles,
            'licenseuse'        => \$licenseuse,
            'logins'            => \$logins,
            'generic'           => \$generic,
            'cmdball'           => \$cmdbAll,
            'cmdbcommon'        => \$cmdbCommon,
            'fpnamp'            => \$fp_namp,
            'fppws'             => \$fppws,
            'fblayout=s'        => \$fblayout,
            'fultextcols'       => \$fultextcols,
            'relationship:s'    => \$relationship,
            'keepshared'        => \$keepshared,
            'smconnectors'      => \$smconnectors
          );

if ($help)
{
    print "\n";
    print "valid arguments: \n";
    print "\n--- Project/System Related ---\n";
    print "-noabmaster          do not load the ABMASTER table\n";
    print "-cal                 only load the calendar tables\n";
    print "-users               only load the user table\n";
    print "-user_profiles       only load the user_profiles table\n";
    print "-licenseuse          only load the license use table\n";
    print "-logins              only load the logins table\n";
    print "-generic             only load the FPGenericConfigTable table\n";
    print "-fpnamp              only load the FP_NAMP table\n";
    print "-fppws               only load the FPPWS tables\n";
    print "-fblayout            only load the FBLAYOUT table\n";
    print "-fultextcols         only load the FullTextExcludeCols table\n";
    print "-relationship[=n]    only load the MASTER_RelationshipMember table\n";
    print "-smconnectors        only load the SMConnectors table\n";
    
    print "\n--- $FP::CMDB_WORD Related ---\n";
    print "-cmdball             only load the cmdb tables (for all CMDB*.xml files except CMDB_MAIN.xml and CMDB_PROJECTS.xml)\n";
    print "-cmdbcommon          only load the system wide CMDB tables (CMDB_MAIN and CMDB_PROJECTS)\n";

    print "\n--- Misc ---\n";
    print "-debug               display debug information\n";
    print "-truncate            delete existing rows from tables before loading data into them\n";
    #print "-keepshared          recommend use with \"-truncate\". Doesn't delete shared tables: FBLayout, MASTER_RelationshipMember, FPTransactionData, FPNAMPOR_state_table, FPNAMPOR_release_table\n";
    print "-keepshared          recommend use with \"-truncate\". Doesn't delete shared tables: FBLayout, MASTER_RelationshipMember, FPTransactionData\n";
    print "-xmldir              path to load xml files from\n";
    print "-help                show this help\n";
    print "\n";
    exit;
}

# cmmaster value (could be bad) checked immediately after
if (!$xmldir)
{
    $xmldir = "${FP::CMI}${FP::FS}db${FP::FS}";
}

local our %shared_tables = (
	'fblayout' => {
		'WSID_FNAME' => 'WsID' 				#We need this field for -truncate param.
	},
	# 'fpnampor_release_table' => {
		# 'WSID_FNAME' => ''
	# },
	# 'fpnampor_state_table' => {
		# 'WSID_FNAME' => ''
	# },
	'fptransactiondata' => {
		'WSID_FNAME' => 'WsID'
	},
	'master_relationshipmember' => {
		'WSID_FNAME' => 'PROJECT_ID'
	}
);

&XMLDBI::ArgAndEnvChecks();
&XMLDBI::FindLoadTables();

sub truncateTable
{
    my $table = shift || undef;    
    my $wsid_field = shift || undef;
    my $wsid_value = shift || undef;
    return unless $table;
    
    #Do not truncate the table twice
    return if exists $alreadyTruncated{$table};

    print "Truncating $table\n";    
    
    #Check related tables, if any truncate them first
    if ( exists $relatedTables{$table} && scalar @{$relatedTables{$table}} )
    {
        foreach my $rTable (@{$relatedTables{$table}})
        {
			if ($main::keepshared && $shared_tables{lc($rTable)}->{'WSID_FNAME'} ne ''){
				my $Rwsid_field = $shared_tables{lc($rTable)}->{'WSID_FNAME'};
				&truncateTable($rTable, $Rwsid_field, $wsid_value);
			}else{
				&truncateTable($rTable);
			}
        }
    }
        
	#Delete all workspaces records from shared table
	if ( $wsid_field && $wsid_value && exists $shared_tables{lc($table)} && $shared_tables{lc($table)}{'WSID_FNAME'} ne ''){
		$FP::dbh->do("DELETE FROM $table WHERE $wsid_field = '$wsid_value'");
	}else{
		#Delete all data from table
		$FP::dbh->do("DELETE FROM $table");
	}
    $alreadyTruncated{$table} = 1; #Flag table as truncated table
}

my $filename;
foreach my $currentTable (@loadTables)
{
    $filename = $main::loadFiles{$currentTable} || $currentTable;
    # doesn't delete info from this shared tables
        #FPNAMPOR_release_table|
        #FPNAMPOR_state_table|
    if ($main::keepshared && $currentTable =~ m/ FBLayout | FPTransactionData | MASTER_RelationshipMember /xi)
    {
		#in case we want to truncate shared table
		if ($main::truncate && $shared_tables{lc($currentTable)}->{'WSID_FNAME'} ne ''){
			my $wsid = $fblayout || $relationship || undef;
			#if this script runs by mrXMLConversation.pl
			unless( $wsid ){
				$ENV{'CMMASTER'} =~ /MASTER(\d+)/;
				$wsid = $1;
			}
			
			&truncateTable($currentTable, $shared_tables{lc($currentTable)}->{'WSID_FNAME'}, $wsid);
		}
		
        next;
    }
    
    if ($main::truncate)
    {
        &truncateTable($currentTable);
    }
} continue
{
    my $xmldb = XMLDBI->new("$currentTable");
    my $xmlFile = "${main::xmldir}${filename}.xml";
    
    print "Populating $currentTable from $xmlFile\n";

    eval
    {
        $xmldb->parsefile($xmlFile);
    };
    if ($@)
    {
        die("Error reading/parsing data file \"$xmlFile\".  $!  --  Exiting.\n");
    }
}

exit;

