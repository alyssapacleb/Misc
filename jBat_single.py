import os, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Take input and fix all of the non-standardized string values
class StandardizeFn(beam.DoFn):
    def process(self, element):
        value = element

        #retrieve all columns
        index = value.get('index')
        Year = value.get('Year')
        City = value.get('City')
        Team = value.get('Team')
        TeamID = value.get('TeamID')
        LName = value.get('LName')
        FName = value.get('FName')
        Avg = value.get('Avg')
        SLG = value.get('SLG')
        OBP = value.get('OBP')
        RiSP = value.get('RiSP')
        G = value.get('G')
        AB = value.get('AB')
        R = value.get('R')
        H = value.get('H')
        _2B = value.get('_2B')
        _3B = value.get('_3B')
        HR = value.get('HR')
        RBI = value.get('RBI')
        BB = value.get('BB')         
        HBP = value.get('HBP')
        SO = value.get('SO')
        SB = value.get('SB')
        GIDP = value.get('GIDP')
        PA = value.get('PA')
        Sac = value.get('Sac')
        E = value.get('E')
        PlayerID = value.get('PlayerID')
        Uniform = value.get('Uniform')
        ARISP = value.get('ARISP')
        SF = value.get('SF')
        SH = value.get('SH')
        DP = value.get('DP')
        Bats = value.get('Bats')
        LG = value.get('LG')
        Throws = value.get('Throws')
        RC_27 = value.get('RC_27')
        A = value.get('A')
        Pos1 = value.get('Pos1')
        Pos = value.get('Pos')
        D_G = value.get('D_G')
        PO = value.get('PO')
        CS = value.get('CS')
        playerpk = value.get('playerpk')

        #clean up the multitude of issues. 
        if Team == 'Marines':
            TeamID = 'CM'
            City = 'Chiba'
            LG = 'P'    
        if Team == 'Dragons':
            City = 'Nagoya'
            TeamID = 'ND'
            LG = 'C'  
        if Team == 'Tigers':
            TeamID = 'NT'
            City = 'Nishinomiya'
            LG = 'C'        
        if Team == 'Hawks':
            TeamID = 'FH'
            City = 'Fukuoka'
            LG = 'P'
        if Team == 'Carp':
            City = 'Hiroshima'
            TeamID = 'HC'
            LG = 'C'
        if Team == 'Bay Stars':
            City = 'Yokohama'
            TeamID = 'YBS'
            LG = 'C'
        if Team == 'Whales':
            City = 'Yokohama'
            TeamID = 'YW'
        if City == 'Yomiuri':
            TeamID = 'YG'
            LG = 'C'
        if City == 'Kintetsu' or City == 'Osaka Kintetsu':
            City = 'Osaka'
            TeamID = 'OB'
            LG = 'P'
        if Team == 'Fighters':
            City = 'Sapporo'
            TeamID = 'SF'
            LG = 'P'
        if City == 'Sankei':
            City = 'Tokyo'
            TeamID = 'TA'
        if Team == 'Swallows':
            TeamID = 'TS'
            City = 'Tokyo'
            LG = 'C'
        if Team == 'Lions':
            City = 'Tokorozawa'
            TeamID = 'TL'
            LG = 'P'
        if City == 'Orix' and Team == 'Blue Wave': 
            City = 'Kobe'
            TeamID = 'KBW'
            LG = 'P'
        if City == 'Orix' and Team == 'Buffaloes': 
            City = 'Osaka'
            TeamID = 'OB'
            LG = 'P'
        if City == 'Rakuten':
            City = 'Sendai'
            TeamID = 'SGE'
            LG = 'P'
        
        tyID = str(TeamID) + str(Year)
        
        #return a tuple/dictionary with the primary key as the key. 
        return [(index,[Year, City, Team, TeamID, LName, FName, Avg, SLG, OBP, RiSP, G, AB, R, H, _2B, _3B, HR, RBI, BB, HBP, SO, SB, GIDP, PA, Sac, E, PlayerID, Uniform, ARISP, SF, SH, DP, Bats, LG, Throws, RC_27, A, Pos1, Pos, D_G, PO, CS, playerpk, tyID])]

# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        index, obj = element # obj is an _UnwindowedValues type
        val = list(obj) # Convert the unwindowed value into a list so I can grab the values
        val = val[0] # For some reason, it is a nested list so I need to retrieve the inner list
        return [val]  
    
        # Retrieved the columns from the list
        '''Year= val[0]
        City = val[1]
        Team = val[2]
        TeamID = val[3]
        LName = val[4]
        FName = val[5]
        Avg = val[6]
        SLG = val[7]
        OBP = val[8]
        RiSP = val[9]
        G = val[10]
        AB = val[11]
        R = val[12]
        H = val[13]
        _2B = val[14]
        _3B = val[15]
        HR = val[16]
        RBI = val[17]
        BB = val[18]
        HBP = val[19]
        SO =  val[20]
        SB =val[21]
        GIDP = val[22]
        PA = val[23]
        Sac = val[24]
        E = val[25]
        playerID = val[26]
        Uniform = val[27]
        ARISP = val[28]
        SF = val[29]
        SH = val[30]
        DP = val[31]
        Bats = val[32]
        LG = val[33]
        Throws = val[34]
        RC_27 = val[35]
        A = val[36]
        Pos1 = val[37]
        Pos = val[38]
        D_G = val[39]
        PO = val[40]
        CS = val[41]
        playerpk = val[42]
        tyID = val[43]

        
        # Made the BQ record with this dictionary within a list.
        record = ['index':index, 'Year':Year, 'City':City, 'Team':Team, 'TeamID':TeamID, 'LName':LName, 'FName':FName, 'Avg':Avg, 'SLG':SLG, 'OBP':OBP, 'RiSP':RiSP, 'G':G, 'AB':AB, 'R':R, 'H':H, '_2B':_2B, '_3B':_3B, 'HR':HR, 'RBI':RBI, 'BB':BB, 'HBP':HBP, 'SO':SO, 'SB':SB, 'GIDP':GIDP, 'PA':PA, 'Sac':Sac, 'E':E, 'playerID': playerID, 'Uniform':Uniform, 'ARISP':ARISP, 'SF':SF, 'SH':SH, 'DP':DP, 'Bats':Bats, 'LG':LG, 'Throws':Throws, 'RC_27':RC_27, 'A':A, 'Pos1':Pos1, 'Pos':Pos, 'D_G':D_G, 'PO':PO, 'CS':CS, 'playerpk':playerpk, 'tyID':tyID]

        return [record] '''


def run():
    PROJECT_ID = 'extended-ascent-252921'

    # Project ID is required when using the BQ source
    options = {
        'project': PROJECT_ID
      }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT * FROM sabermetrics_workflow_modeled.jBat'
  
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=sql, use_standard_sql=True))

    # apply a ParDo to the PCollection 
    players_pcoll = query_results | 'Extract Players' >> beam.ParDo(StandardizeFn())

    # apply GroupByKey to the PCollection
    intermediate_pcoll = players_pcoll | 'Group by players' >> beam.GroupByKey()
    
    # Manipulate the file to send to BQ
    done = intermediate_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())  
    
    dataset_id = 'sabermetrics_workflow_modeled'
    table_id = 'jBat_Beam'
    schema_id = 'index:INTEGER, Year:INTEGER, City:STRING, Team:STRING, TeamID:STRING, LName:STRING, FName:STRING, Avg:FLOAT, SLG:FLOAT, OBP:FLOAT, RiSP:FLOAT, G:INTEGER, AB:INTEGER, R:INTEGER, H:INTEGER, _2B:INTEGER, _3B:INTEGER, HR:INTEGER, RBI:INTEGER, BB:INTEGER, HBP:INTEGER, SO:INTEGER, SB:INTEGER, GIDP:INTEGER, PA:INTEGER, Sac:INTEGER, E:INTEGER, playerID:STRING, Uniform:INTEGER, ARISP:FLOAT, SF:INTEGER, SH:INTEGER, DP:INTEGER, Bats:STRING, LG:STRING, Throws:STRING, RC_27:FLOAT, A:INTEGER, Pos1:INTEGER, Pos:STRING, D_G:INTEGER, PO:INTEGER, CS:INTEGER, playerpk:STRING, tyID:STRING'

    # write PCollection to new BQ table
    done | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                table=table_id, 
                schema=schema_id,
                project=PROJECT_ID,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                batch_size=int(100))

result = p.run()
result.wait_until_finish()
