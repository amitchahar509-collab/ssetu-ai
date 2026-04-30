#!/usr/bin/env python3
"""Generates primary_data_datasets.csv for S-SETU — run once if CSV not uploaded."""
import csv, random, datetime, os

COMPANIES = [
    ("L17110MH1973PLC019786","RELIANCE INDUSTRIES LTD","Active","08/05/1973","27AABCR1931F1ZV","Active",96,2,"45","0","No","4.8"),
    ("L72200MH2009PLC198741","DEMO INFRASTRUCTURE PVT LTD","Struck-off","15/03/2009","27AABCD1234E1Z5","Cancelled",22,2,"3","2","Yes","1.2"),
    ("L85110KA1981PLC013115","INFOSYS LIMITED","Active","02/07/1981","29AABCI1681G1ZF","Active",97,0,"12","0","No","4.9"),
    ("L22210MH1995PLC084781","TATA CONSULTANCY SERVICES LTD","Active","19/01/1995","27AAACT2727Q1ZX","Active",99,0,"45","0","No","4.9"),
    ("U72200DL2016PUB198000","GHOST VENTURES PVT LTD","Not Found","","","Not Found",0,0,"0","0","No","0"),
    ("L55200DL2010PLC201234","SAMPLE ENTERPRISES PVT LTD","Active","10/06/2010","07AABCS1234F1Z6","Active",74,1,"5","0","No","3.8"),
    ("U36900MH2014PTC258741","BHARAT AGRO TECH PVT LTD","Dormant","12/11/2014","27AABCB2014D1ZP","Suspended",45,3,"2","1","No","2.1"),
    ("L24110GJ1965PLC001239","GUJARAT FERTILIZERS CO LTD","Active","22/03/1965","24AABCG1965F1ZR","Active",88,0,"8","0","No","4.3"),
    ("U45200KA2018PTC110021","SHELL BUILD CONSTRUCTIONS","Active","05/08/2018","29AABCS2018P1ZQ","Active",31,5,"7","3","Yes","1.8"),
    ("L67120MH2001PLC132765","MAHARASHTRA FINSERV LTD","Active","14/02/2001","27AABCM2001H1ZS","Active",82,0,"15","0","No","4.1"),
    ("U74999DL2020OPC372819","NEWAGE DIGITAL SOLUTIONS OPC","Active","30/11/2020","07AABCN2020Q1ZT","Active",91,0,"3","0","No","4.6"),
    ("L51100WB1992PLC055212","KOLKATA PORT LOGISTICS LTD","Active","17/07/1992","19AABCK1992L1ZU","Active",79,1,"22","1","No","3.5"),
    ("U99999MH2022PTC400001","FAKE EXPORT IMPORT PVT LTD","Struck-off","01/01/2022","27AABCF2022X1ZV",  "Cancelled",8,4,"0","0","Yes","0"),
    ("L40300AP1974PLC001850","AP POWER GENERATION CORP","Active","03/09/1974","37AABCA1974P1ZW","Active",93,0,"30","0","No","4.7"),
    ("U70200HR2019PTC078431","HARYANA INFRA PROJECTS PVT","Active","22/04/2019","06AABCH2019R1ZX","Active",66,2,"4","1","No","3.2"),
]

HEADER = [
    "cin","company_name","company_status","date_of_incorporation",
    "gstin","gst_status","gst_compliance_pct","late_filings",
    "total_contracts","contract_defaults","blacklisted","avg_performance_rating",
    # Extra enrichment columns
    "authorized_capital_lac","paid_up_capital_lac",
    "last_balance_sheet_date","last_agm_date",
    "state","industry_sector","annual_turnover_slab",
    "transparency_gap_score","data_source","last_updated"
]

SECTORS  = ["Infrastructure","IT/Technology","Finance","Agriculture","Manufacturing","Logistics","Energy","Export/Import"]
STATES   = ["Maharashtra","Delhi","Karnataka","Gujarat","West Bengal","Andhra Pradesh","Haryana","Tamil Nadu"]
SLABS    = ["<40L","40L-1.5Cr","1.5Cr-5Cr","5Cr-10Cr","10Cr-50Cr","50Cr+"]

random.seed(42)
rows = []
for row in COMPANIES:
    cin,name,status,incorp,gstin,gst,comp,late,contracts,defaults,blacklisted,rating = row
    auth_cap = round(random.uniform(1, 5000), 2)
    paidup   = round(auth_cap * random.uniform(0.01, 1.0), 2)
    # Synthetic dates
    if incorp:
        try:
            d = datetime.datetime.strptime(incorp, "%d/%m/%Y")
            bs_lag = random.randint(365, 900)
            bs_date = (d + datetime.timedelta(days=max(365*2, (datetime.date.today()-d.date()).days - bs_lag))).strftime("%d/%m/%Y")
            agm_lag = random.randint(300, 700)
            agm_date= (d + datetime.timedelta(days=max(365, (datetime.date.today()-d.date()).days - agm_lag))).strftime("%d/%m/%Y")
        except:
            bs_date = agm_date = ""
    else:
        bs_date = agm_date = ""

    # Compute transparency gap (inverse of integrity signals)
    tgap = 0
    if "active" not in status.lower():   tgap += 25
    if "active" not in gst.lower():      tgap += 20
    if int(defaults) > 0:                tgap += int(defaults) * 10
    if blacklisted == "Yes":             tgap += 30
    if int(comp) < 50:                   tgap += 15
    tgap = min(100, tgap)

    rows.append([
        cin, name, status, incorp,
        gstin, gst, comp, late,
        contracts, defaults, blacklisted, rating,
        auth_cap, paidup,
        bs_date, agm_date,
        random.choice(STATES), random.choice(SECTORS), random.choice(SLABS),
        tgap, "MCA21+GST+GeM", datetime.date.today().isoformat()
    ])

out_path = "/home/claude/ssetu_integrated/primary_data_datasets.csv"
with open(out_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(HEADER)
    writer.writerows(rows)

print(f"✓ Generated {len(rows)} rows → {out_path}")
