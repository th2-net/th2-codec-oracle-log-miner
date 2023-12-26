# th2-codec-oracle-log-miner (0.0.1)

## Description

Designed for transform parsed message at the end of the pipeline
1. [th2-read-db which pulls Oracle redo log](https://github.com/th2-net/th2-read-db/tree/dev-2/oracle-log-miner.md)
2. [th2-codec-csv](https://github.com/th2-net/th2-codec-csv)

The codec (transformer) is based on [th2-codec](https://github.com/th2-net/th2-codec). You can find additional information [here](https://github.com/th2-net/th2-codec/blob/master/README.md)

## Decoding (transformation)

The codec parses a SQL query in the `SQL_REDO` field to extract column and value pairs and appends them into an output message.
The `OPERATION`, `SQL_REDO`, `ROW_ID`, `TIMESTAMP`, `TABLE_NAME` fields are required in source parsed message for transformation otherwise `th2-codec-error` message is produced 

### Parser workflow:
1. detects type of query by the `OPERATION` field. 
Currently, the `INSERT` / `UPDATE` / `DELETE` values are supported.
2. parses a value of the `SQL_REDO` field as SQL query.
3. creates an empty parsed message and copies the whole metadata from the source message.
4. appends parsed columns and values to the new parsed message. 
Each field will have a prefix specified in the `columnPrefix` option.
5. copies fields specified the `saveColumns` option from source to new parsed message.

### Transform example:

source parsed message body
```json
{
  "SCN": 3627285,
  "TIMESTAMP": "06-Dec-2023 08:54:31",
  "OPERATION": "UPDATE",
  "TABLE_NAME": "EMPLOYEE",
  "ROW_ID": "AAASt5AAHAAAAFcAAA",
  "SQL_REDO": "update \"<user>\".\"EMPLOYEE\" set \"SAVINGS\" = '10' where \"SAVINGS\" = '1' and ROWID = 'AAASt5AAHAAAAFcAAA';"
}
```

outgoing parsed message body after transform with default configuration
```json
{
  "TIMESTAMP": "06-Dec-2023 08:54:31",
  "OPERATION": "UPDATE",
  "TABLE_NAME": "EMPLOYEE",
  "ROW_ID": "AAASt5AAHAAAAFcAAA",
  "SQL_REDO": "update \"<user>\".\"EMPLOYEE\" set \"SAVINGS\" = '10' where \"SAVINGS\" = '1' and ROWID = 'AAASt5AAHAAAAFcAAA';",
  "th2_SAVINGS": "10"
}
```

## Encode

This operation isn't support.

## Settings

Oracle log miner codec (transformer) has the following parameters:
```yaml
columnPrefix: 'th2_'
saveColumns: [ OPERATION, SQL_REDO, ROW_ID, TIMESTAMP, TABLE_NAME ]
```

**columnPrefix** - prefix for parsed columns.
**saveColumns** - set of column names to copy from source message.
All columns which log miner allow to select are described in the [document](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-LOGMNR_CONTENTS.html#GUID-B9196942-07BF-4935-B603-FA875064F5C3) 

## Full configuration example

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec-oracle-log-miner
spec:
  imageName: ghcr.io/th2-net/th2-codec-oracle-log-miner
  imageVersion: 0.0.1
  customConfig:
    transportLines:
      rpt:
        type: TH2_TRANSPORT
        useParentEventId: false    
    # required true to decode (transform) parsed message 
    disableMessageTypeCheck: true
    disableProtocolCheck: true
    
    codecSettings:
      column-prefix: th2_
      save-columns:
        - OPERATION
        - SQL_REDO
        - TIMESTAMP
        - ROW_ID
        - TABLE_NAME
  pins:
    grpc:
      server:
        - name: server
          serviceClasses:
            - com.exactpro.th2.codec.grpc.CodecService
    mq:
      subscribers:
        - name: lwdp_in_codec_encode
          attributes: [ transport-group, subscribe, lwdp_encoder_in ]
        - name: lwdp_in_oracle_log_miner
          attributes:
            - transport-group
            - subscribe
            - lwdp_decoder_in

        - name: rpt_in_codec_encode
          attributes: [ transport-group, subscribe, rpt_encoder_in ]
        - name: rpt_in_oracle_log_miner
          attributes:
            - transport-group
            - subscribe
            - rpt_decoder_in

      publishers:
        - name: lwdp_out_codec_encode
          attributes: [ transport-group, publish, lwdp_encoder_out ]
        - name: lwdp_out_codec_decode
          attributes:
            - transport-group
            - publish
            - lwdp_decoder_out

        - name: rpt_out_codec_encode
          attributes: [ transport-group, publish, rpt_encoder_out ]
        - name: rpt_out_codec_decode
          attributes:
            - transport-group
            - publish
            - rpt_decoder_out
```

## Release notes

### 0.0.1
+ Parse `INSERT` / `UPDATE` / `DELETE` SQL queries from the `SQL_REDO` field.
+ Copy specified fields from source to output parsed message.