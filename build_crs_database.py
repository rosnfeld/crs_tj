"""
For building a PostgreSQL database tables to house CRS data for the web app.
"""

import psycopg2


def get_db_connection(host, database, user, password):
    return psycopg2.connect(host=host, database=database, user=user, password=password)


def create_crs_table(connection):
    cursor = connection.cursor()

    sql = '''CREATE TABLE crs (
        year integer,
        donorcode integer,
        donorname varchar (63),
        agencycode integer,
        agencyname varchar (63),
        crsid varchar (63),
        projectnumber varchar (63),
        initialreport integer,
        recipientcode integer,
        recipientname varchar (63),
        regioncode integer,
        regionname varchar (63),
        incomegroupcode integer,
        incomegroupname varchar (63),
        flowcode integer,
        flowname varchar (63),
        bi_multi integer,
        category integer,
        finance_t integer,
        aid_t varchar (63),
        usd_commitment double precision,
        usd_disbursement double precision,
        usd_received double precision,
        usd_commitment_defl double precision,
        usd_disbursement_defl double precision,
        usd_received_defl double precision,
        usd_adjustment double precision,
        usd_adjustment_defl double precision,
        usd_amountuntied double precision,
        usd_amountpartialtied double precision,
        usd_amounttied double precision,
        usd_amountuntied_defl double precision,
        usd_amountpartialtied_defl double precision,
        usd_amounttied_defl double precision,
        usd_IRTC double precision,
        usd_expert_commitment double precision,
        usd_expert_extended double precision,
        usd_export_credit double precision,
        currencycode integer,
        commitment_national double precision,
        disbursement_national double precision,
        shortdescription text,
        projecttitle text,
        purposecode integer,
        purposename varchar (63),
        sectorcode integer,
        sectorname varchar (63),
        channelcode double precision,
        channelname varchar (63),
        channelreportedname text,
        geography text,
        expectedstartdate varchar (63),
        completiondate varchar (63),
        longdescription text,
        gender double precision,
        trade double precision,
        FTC double precision,
        PBA double precision
    );'''

