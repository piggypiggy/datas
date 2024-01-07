import os
import numpy as np
import pandas as pd
import datetime
from utils import *
from fredapi import Fred


# https://www.bls.gov/cps/definitions.htm

ids = [ 
      ## Current Population Survey (Household Survey)

      # Unemployment Rate
      ['UNRATE', 'Unemployment Rate'],
      ['UNRATENSA', 'Unemployment Rate NSA'],

      ['LNS14000006', 'Unemployment Rate - Black or African American'],
      ['LNU04000006', 'Unemployment Rate - Black or African American NSA'],
      ['LNS14000009', 'Unemployment Rate - Hispanic or Latino'],
      ['LNU04000009', 'Unemployment Rate - Hispanic or Latino NSA'],
      ['LNS14000003', 'Unemployment Rate - White'],
      ['LNU04000003', 'Unemployment Rate - White NSA'],
      ['LNS14032183', 'Unemployment Rate - Asian'],
      ['LNU04032183', 'Unemployment Rate - Asian NSA'],

      ['LNS14000001', 'Unemployment Rate - Men'],
      ['LNU04000001', 'Unemployment Rate - Men NSA'],
      ['LNS14000002', 'Unemployment Rate - Women'],
      ['LNU04000002', 'Unemployment Rate - Women NSA'],

      ['LNS14024887', 'Unemployment Rate - 16-24 Yrs.'],
      ['LNU04024887', 'Unemployment Rate - 16-24 Yrs. NSA'],
      ['LNS14000060', 'Unemployment Rate - 25-54 Yrs.'],
      ['LNU04000060', 'Unemployment Rate - 25-54 Yrs. NSA'],
      ['LNS14000024', 'Unemployment Rate - 20 Yrs. & over'],
      ['LNU04000024', 'Unemployment Rate - 20 Yrs. & over NSA'],

      ['LNS14000031', 'Unemployment Rate - 20 Yrs. & over, Black or African American Men'],
      ['LNU04000031', 'Unemployment Rate - 20 Yrs. & over, Black or African American Men NSA'],
      ['LNS14000032', 'Unemployment Rate - 20 Yrs. & over, Black or African American Women'],
      ['LNU04000032', 'Unemployment Rate - 20 Yrs. & over, Black or African American Women NSA'],

      ['LNU04073413', 'Unemployment Rate - Native Born NSA'],
      ['LNU04073395', 'Unemployment Rate - Foreign Born NSA'],

      ['LNS14027659', 'Unemployment Rate - Less Than a High School Diploma, 25 Yrs. & over'],
      ['LNU04027659', 'Unemployment Rate - Less Than a High School Diploma, 25 Yrs. & over NSA'],
      ['LNS14027660', 'Unemployment Rate - High School Graduates, No College, 25 Yrs. & over'],
      ['LNU04027660', 'Unemployment Rate - High School Graduates, No College, 25 Yrs. & over NSA'],
      ['LNS14027662', 'Unemployment Rate - Bachelor\'s Degree and Higher, 25 Yrs. & over'],
      ['LNU04027662', 'Unemployment Rate - Bachelor\'s Degree and Higher, 25 Yrs. & over NSA'], 
      ['CGBD2534', 'Unemployment Rate - College Graduates - Bachelor\'s Degree, 25 to 34 years'],
      ['CGBD25O', 'Unemployment Rate - College Graduates - Bachelor\'s Degree, 25 years and over'],
      ['CGMD25O', 'Unemployment Rate - College Graduates - Master\'s Degree, 25 years and over'],
      ['CGDD25O', 'Unemployment Rate - College Graduates - Doctoral Degree, 25 years and over'],

      ['LNU04032231', 'Unemployment Rate - Construction Industry, Private Wage and Salary Workers NSA'],
      ['LNU04032232', 'Unemployment Rate - Manufacturing Industry, Private Wage and Salary Workers NSA'],
      ['LNU04035109', 'Unemployment Rate - Agricultural and Related Private Wage and Salary Workers NSA'],
      ['LNU04032230', 'Unemployment Rate - Mining, Quarrying, and Oil and Gas Extraction, Nonagricultural Private Wage and Salary Workers NSA'],
      ['LNU04032235', 'Unemployment Rate - Wholesale and Retail Trade, Private Wage and Salary Workers NSA'],
      ['LNU04032241', 'Unemployment Rate - Leisure and Hospitality, Private Wage and Salary Workers NSA'],
      ['LNU04032240', 'Unemployment Rate - Education and Health Services, Private Wage and Salary Workers NSA'],
      ['LNU04032238', 'Unemployment Rate - Financial Activities Industry, Private Wage and Salary Workers NSA'],
      ['LNU04032216', 'Unemployment Rate - Management, Business, and Financial Operations Occupations NSA'],
      ['LNU04032236', 'Unemployment Rate - Transportation and Utilities Industry, Private Wage and Salary Workers NSA'],
      ['LNU04032237', 'Unemployment Rate - Information Industry, Private Wage and Salary Workers NSA'],
      ['LNU04032239', 'Unemployment Rate - Professional and Business Services Industry, Private Wage and Salary Workers NSA'],
      ['LNU04032233', 'Unemployment Rate - Durable Goods Industry, Private Wage and Salary Workers NSA'],
      ['LNU04035181', 'Unemployment Rate - All Industries, Self-Employed, Unincorporated, and Unpaid Family Workers NSA'],
      ['LNU04028615', 'Unemployment Rate All Industries Government Wage & Salary Workers NSA'],
      ['LNS14200000', 'Unemployment Rate Part-Time Workers'],

      # Civilian Labor Force, 失业率对照的基数
      ['CLF16OV', 'Civilian Labor Force Level'],
      ['LNU01000000', 'Civilian Labor Force Level NSA'],

      ['LNS11000001', 'Civilian Labor Force Level - Men'],
      ['LNU01000001', 'Civilian Labor Force Level - Men NSA'],
      ['LNS11000002', 'Civilian Labor Force Level - Women'],
      ['LNU01000002', 'Civilian Labor Force Level - Women NSA'],

      ['LNU01073395', 'Civilian Labor Force Level - Foreign Born'],
      ['LNU01073413', 'Civilian Labor Force Level - Native Born'],
    
      ['LNS11000012', 'Civilian Labor Force Level - 16-19 Yrs.'],
      ['LNU01000012', 'Civilian Labor Force Level - 16-19 Yrs. NSA'],
      ['LNS11000036', 'Civilian Labor Force Level - 20-24 Yrs.'],
      ['LNU01000036', 'Civilian Labor Force Level - 20-24 Yrs. NSA'],
      ['LNS11000060', 'Civilian Labor Force Level - 25-54 Yrs.'],
      ['LNU01000060', 'Civilian Labor Force Level - 25-54 Yrs. NSA'],
      ['LNS11024230', 'Civilian Labor Force Level - 55 Yrs. & over'],
      ['LNU01024230', 'Civilian Labor Force Level - 55 Yrs. & over NSA'],

      # Civilian Labor Force Participation Rate
      # “劳动力人口(civilian labor force)”与“适合工作人口(civilian noninstitutional population)”之比，
      # 反映的是适龄人口中愿意加入就业市场并持续寻找工作的劳动力比率
      ['CIVPART', 'Labor Force Participation Rate'],
      ['LNU01300000', 'Labor Force Participation Rate NSA'],

      ['LNS11300012', 'Labor Force Participation Rate - 16-19 Yrs.'],
      ['LNU01300012', 'Labor Force Participation Rate - 16-19 Yrs.'],
      ['LNS11300036', 'Labor Force Participation Rate - 20-24 Yrs.'],
      ['LNU01300036', 'Labor Force Participation Rate - 20-24 Yrs. NSA'],
      ['LNS11300060', 'Labor Force Participation Rate - 25-54 Yrs.'],
      ['LNU01300060', 'Labor Force Participation Rate - 25-54 Yrs. NSA'],
      ['LNS11324230', 'Labor Force Participation Rate - 55 Yrs. & over'],
      ['LNU01324230', 'Labor Force Participation Rate - 55 Yrs. & over NSA'],

      ['LNU01373413', 'Labor Force Participation Rate - Native Born'],
      ['LNU01373395', 'Labor Force Participation Rate - Foreign Born'],

      ['LNS11300006', 'Labor Force Participation Rate - Black or African American'],
      ['LNU01300006', 'Labor Force Participation Rate - Black or African American NSA'],
      ['LNS11300003', 'Labor Force Participation Rate - White'],
      ['LNU01300003', 'Labor Force Participation Rate - White NSA'],
      ['LNU01332183', 'Labor Force Participation Rate - Asian NSA'],
      ['LNS11300009', 'Labor Force Participation Rate - Hispanic or Latino'],
      ['LNU01300009', 'Labor Force Participation Rate - Hispanic or Latino NSA'],

      # Employment Population Ratio & Employment
      # proportion of the civilian noninstitutional population that is employed.
      ['EMRATIO', 'Employment-Population Ratio'],
      ['LNU02300000', 'Employment-Population Ratio NSA'],
      
      ['LNS12300012', 'Employment-Population Ratio - 16-19 Yrs.'],
      ['LNU02300012', 'Employment-Population Ratio - 16-19 Yrs. NSA'],
      ['LNS12000012', 'Employment Level - 16-19 Yrs.'],
      ['LNU02000012', 'Employment Level - 16-19 Yrs. NSA'],
      ['LNS12000036', 'Employment Level - 20-24 Yrs.'],
      ['LNU02000036', 'Employment Level - 20-24 Yrs. NSA'],
      ['LNS12000060', 'Employment Level - 25-54 Yrs.'],
      ['LNU02000060', 'Employment Level - 25-54 Yrs. NSA'],
      ['LNS12300060', 'Employment-Population Ratio - 25-54 Yrs.'],
      ['LNS12024230', 'Employment Level - 55 Yrs. & over'],
      ['LNU02024230', 'Employment Level - 55 Yrs. & over NSA'],

      ['LNU02073413', 'Employment Level - Native Born'],
      ['LNU02073395', 'Employment Level - Foreign Born'],
      ['LNU02373413', 'Employment-Population Ratio - Native Born'],
      ['LNU02373395', 'Employment-Population Ratio - Foreign Born'],

      ['LNS12300006', 'Employment-Population Ratio - Black or African American'],
      ['LNU02300006', 'Employment-Population Ratio - Black or African American NSA'],
      ['LNS12300003', 'Employment-Population Ratio - White'],
      ['LNU02300003', 'Employment-Population Ratio - White NSA'],
      ['LNU02332183', 'Employment Population Ratio - Asian'],
      ['LNS12300009', 'Employment-Population Ratio - Hispanic or Latino'],
      ['LNU02300009', 'Employment-Population Ratio - Hispanic or Latino NSA'],

      ['LNS12500000', 'Employed, Usually Work Full Time'],
      ['LNU02500000', 'Employed, Usually Work Full Time NSA'],
      ['LNS12600000', 'Employed, Usually Work Part Time'],
      ['LNU02600000', 'Employed, Usually Work Part Time NSA'],

      # Multiple Jobholders
      # Employed persons with more than one job
      ['LNS12026620', 'Multiple Jobholders as a Percent of Employed'],
      ['LNU02026620', 'Multiple Jobholders as a Percent of Employed NSA'],
      ['LNS12026619', 'Multiple Jobholders'],
      ['LNU02026619', 'Multiple Jobholders NSA'],

      ['LNU02026631', 'Multiple Jobholders, Primary and Secondary Jobs Both Full Time'],
      ['LNU02026625', 'Multiple Jobholders, Primary Job Full Time, Secondary Job Part Time'], 
      ['LNU02026628', 'Multiple Jobholders, Primary and Secondary Jobs Both Part Time'],
      ['LNU02026634', 'Multiple Jobholders, Hours Vary On Primary Or Secondary Job'],

      # Duration of Unemployment
      ['UEMPMEAN', 'Average Weeks Unemployed'],
      ['LNU03008275', 'Average Weeks Unemployed'],
      ['UEMPMED', 'Median Weeks Unemployed'],
      ['LNU03008276', 'Median Weeks Unemployed'],

      ['UEMPLT5', 'Number Unemployed for Less Than 5 Weeks'],
      ['LNU03008396', 'Number Unemployed for Less Than 5 Weeks NSA'],
      ['UEMP5TO14', 'Number Unemployed for 5-14 Weeks'],
      ['LNU03008756', 'Number Unemployed for 5-14 Weeks NSA'],
      ['UEMP15T26', 'Number Unemployed for 15-26 Weeks'],
      ['LNU03008876', 'Number Unemployed for 15-26 Weeks NSA'],
      ['UEMP27OV', 'Number Unemployed for 27 Weeks & over'],
      ['LNU03008636', 'Number Unemployed for 27 Weeks & over NSA'],

      ['LNS13008397', 'Of Total Unemployed, Percent Unemployed Less Than 5 Weeks'],
      ['LNU03008397', 'Of Total Unemployed, Percent Unemployed Less Than 5 Weeks NSA'],
      ['LNS13025701', 'Of Total Unemployed, Percent Unemployed 5-14 Weeks'],
      ['LNU03025701', 'Of Total Unemployed, Percent Unemployed 5-14 Weeks NSA'],
      ['LNS13025702', 'Of Total Unemployed, Percent Unemployed 15-26 Weeks'],
      ['LNU03025702', 'Of Total Unemployed, Percent Unemployed 15-26 Weeks NSA'],
      ['LNS13025703', 'Of Total Unemployed, Percent Unemployed 27 Weeks & over'],
      ['LNU03025703', 'Of Total Unemployed, Percent Unemployed 27 Weeks & over NSA'],

      # Losers and Leavers
      # Losers: Unemployed workers who have been involuntarily laid off or fired from their jobs
      # Leavers: Unemployed workers who have voluntarily quit their current jobs 
      # and have immediate sought other employment
      ['LNS13023706', 'Job Leavers as a Percent of Total Unemployed'],
      ['LNU03023706', 'Job Leavers as a Percent of Total Unemployed NSA'],
      ['LNS13023622', 'Job Losers as a Percent of Total Unemployed'],
      ['LNU03023622', 'Job Losers as a Percent of Total Unemployed NSA'],
      ['LNS13023654', 'Job Losers on Layoff as a Percent of Total Unemployed'],
      ['LNU03023654', 'Job Losers on Layoff as a Percent of Total Unemployed NSA'],
      ['LNS13026511', 'Job Losers Not on Layoff as a Percent of Total Unemployed'],
      ['LNU03026511', 'Job Losers Not on Layoff as a Percent of Total Unemployed NSA'],

      # Entrants and Reentrants
      # Entrants: unemployed people looking for their first job. They have no previous work experience.
      # Reentrants: unemployed people who have past work experience 
      # but were not in the labor force for a period of time prior to beginning their current job search
      ['LNS13023558', 'Reentrants to Labor Force as a Percent of Total Unemployed'],
      ['LNU03023558', 'Reentrants to Labor Force as a Percent of Total Unemployed NSA'],
      ['LNS13023570', 'New Entrants as a Percent of Total Unemployed'],
      ['LNU03023570', 'New Entrants as a Percent of Total Unemployed NSA'],
      
      # Not in Labor Force
      ['LNS15000000', 'Not in Labor Force'],
      ['LNU05000000', 'Not in Labor Force NSA'],

      
      ## Current Employment Statistics (Establishment Survey)
      # 
      ['PAYEMS', 'All Employees, Total Nonfarm'],
      ['PAYNSA', 'All Employees, Total Nonfarm NSA'],
      # Total Private
      ['USPRIV', 'All Employees, Total Private'],
      ['CEU0500000001', 'All Employees, Total Private'],
      ['AWHAETP', 'Average Weekly Hours of All Employees, Total Private'],
      ['CEU0500000002', 'Average Weekly Hours of All Employees, Total Private NSA'],
      ['CES0500000003', 'Average Hourly Earnings of All Employees, Total Private'],
      ['CEU0500000003', 'Average Hourly Earnings of All Employees, Total Private NSA'],
      ['CES0500000006', 'Production and Nonsupervisory Employees, Total Private'],
      ['CEU0500000006', 'Production and Nonsupervisory Employees, Total Private NSA'],
      ['AWHNONAG', 'Average Weekly Hours of Production and Nonsupervisory Employees, Total Private'],
      ['CEU0500000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Total Private NSA'],
      ['AHETPI', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Total Private'],
      ['CEU0500000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Total Private NSA'],
      # Goods-Producing
      ['USGOOD', 'All Employees, Goods-Producing'],
      ['CEU0600000001', 'All Employees, Goods-Producing NSA'],
      ['AWHAEGP', 'Average Weekly Hours of All Employees, Goods-Producing'],
      ['CEU0600000002', 'Average Weekly Hours of All Employees, Goods-Producing NSA'],
      ['CES0600000003', 'Average Hourly Earnings of All Employees, Goods-Producing'],
      ['CEU0600000003', 'Average Hourly Earnings of All Employees, Goods-Producing NSA'],
      ['CES0600000006', 'Production and Nonsupervisory Employees, Goods-Producing'],
      ['CEU0600000006', 'Production and Nonsupervisory Employees, Goods-Producing NSA'],
      ['CES0600000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Goods-Producing'],
      ['CEU0600000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Goods-Producing NSA'],
      ['CES0600000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Goods-Producing'],
      ['CEU0600000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Goods-Producing NSA'],
      # Service-Providing
      ['SRVPRD', 'All Employees, Service-Providing'],
      # Private Service-Providing
      ['CES0800000001', 'All Employees, Private Service-Providing'],
      ['CEU0800000001', 'All Employees, Private Service-Providing NSA'],
      ['AWHAEPSP', 'Average Weekly Hours of All Employees, Private Service-Providing'],
      ['CEU0800000002', 'Average Weekly Hours of All Employees, Private Service-Providing NSA'],
      ['CES0800000003', 'Average Hourly Earnings of All Employees, Private Service-Providing'],
      ['CEU0800000003', 'Average Hourly Earnings of All Employees, Private Service-Providing NSA'],
      ['CES0800000006', 'Production and Nonsupervisory Employees, Private Service-Providing'],
      ['CEU0800000006', 'Production and Nonsupervisory Employees, Private Service-Providing NSA'],
      ['CES0800000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Private Service-Providing'],
      ['CEU0800000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Private Service-Providing NSA'],
      ['CES0800000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Private Service-Providing'],
      ['CEU0800000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Private Service-Providing NSA'],
      # Mining and Logging
      ['USMINE', 'All Employees, Mining and Logging'],
      ['CEU1000000001', 'All Employees, Mining and Logging NSA'],
      ['AWHAEMAL', 'Average Weekly Hours of All Employees, Mining and Logging'],
      ['CEU1000000002', 'Average Weekly Hours of All Employees, Mining and Logging NSA'],
      ['CES1000000003', 'Average Hourly Earnings of All Employees, Mining and Logging'],
      ['CEU1000000003', 'Average Hourly Earnings of All Employees, Mining and Logging NSA'],
      ['CES1000000006', 'Production and Nonsupervisory Employees, Mining and Logging'],
      ['CEU1000000006', 'Production and Nonsupervisory Employees, Mining and Logging NSA'],
      ['CES1000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Mining and Logging'],
      ['CEU1000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Mining and Logging NSA'],
      ['CES1000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Mining and Logging'],
      ['CEU1000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Mining and Logging NSA'],
      ['CES1021100001', 'All Employees, Oil and Gas Extraction'],
      ['CEU1021100001', 'All Employees, Oil and Gas Extraction NSA'],
      # Construction
      ['USCONS', 'All Employees, Construction'],
      ['CEU2000000001', 'All Employees, Construction NSA'],
      ['AWHAECON', 'Average Weekly Hours of All Employees, Constructio'],
      ['CEU2000000002', 'Average Weekly Hours of All Employees, Construction NSA'],
      ['CES2000000003', 'Average Hourly Earnings of All Employees, Construction'],
      ['CEU2000000003', 'Average Hourly Earnings of All Employees, Construction NSA'],
      ['CES2000000006', 'Production and Nonsupervisory Employees, Construction'],
      ['CEU2000000006', 'Production and Nonsupervisory Employees, Construction NSA'],
      ['CES2000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Construction'],
      ['CEU2000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Construction NSA'],
      ['CES2000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Construction'],
      ['AHECONS', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Construction NSA'],
      ['CES2023610001', 'All Employees, Residential Building Construction'],
      ['CEU2023610001', 'All Employees, Residential Building Construction NSA'],
      ['CES2023620001', 'All Employees, Nonresidential Building Construction'],
      ['CEU2023620001', 'All Employees, Nonresidential Building Construction NSA'],
      ['CES2023700001', 'All Employees, Heavy and Civil Engineering Construction'],
      ['CEU2023700001', 'All Employees, Heavy and Civil Engineering Construction NSA'],
      ['CES2023800001', 'All Employees, Specialty Trade Contractors'],
      ['CEU2023800001', 'All Employees, Specialty Trade Contractors NSA'],
      # Manufacturing
      ['MANEMP', 'All Employees, Manufacturing'],
      ['CEU3000000001', 'All Employees, Manufacturing NSA'],
      ['AWHAEMAN', 'Average Weekly Hours of All Employees, Manufacturing'],
      ['CEU3000000002', 'Average Weekly Hours of All Employees, Manufacturing NSA'],      
      ['CES3000000003', 'Average Hourly Earnings of All Employees, Manufacturing'],
      ['CEU3000000003', 'Average Hourly Earnings of All Employees, Manufacturing NSA'],
      ['CES3000000006', 'Production and Nonsupervisory Employees, Manufacturing'],
      ['CEU3000000006', 'Production and Nonsupervisory Employees, Manufacturing NSA'],
      ['AWHMAN', 'Average Weekly Hours of Production and Nonsupervisory Employees, Manufacturing'],
      ['CEU3000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Manufacturing NSA'],
      ['CES3000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Manufacturing'],
      ['AHEMAN', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Manufacturing NSA'],
      # Durable Goods
      ['DMANEMP', 'All Employees, Durable Goods'],
      ['CEU3100000001', 'All Employees, Durable Goods NSA'],
      ['AWHAEDG', 'Average Weekly Hours of All Employees, Durable Goods'],
      ['CEU3100000002', 'Average Weekly Hours of All Employees, Durable Goods NSA'],
      ['CES3100000003', 'Average Hourly Earnings of All Employees, Durable Goods'],
      ['CEU3100000003', 'Average Hourly Earnings of All Employees, Durable Goods NSA'],
      ['CES3100000006', 'Production and Nonsupervisory Employees, Durable Goods'],
      ['CEU3100000006', 'Production and Nonsupervisory Employees, Durable Goods NSA'],
      ['CES3100000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Durable Goods'],
      ['CEU3100000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Durable Goods NSA'],
      ['CES3100000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Durable Goods'],
      ['CEU3100000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Durable Goods NSA'],
      ['CES3133600101', 'All Employees, Motor Vehicles and Parts'],
      ['CEU3133600101', 'All Employees, Motor Vehicles and Parts NSA'],
      ['CES3132700001', 'All Employees, Nonmetallic Mineral Product Manufacturing'],
      ['CEU3132700001', 'All Employees, Nonmetallic Mineral Product Manufacturing NSA'],
      ['CES3133600001', 'All Employees, Transportation Equipment Manufacturing'],
      ['CEU3133600001', 'All Employees, Transportation Equipment Manufacturing NSA'],
      ['CES3133400001', 'All Employees, Computer And Electronic Product Manufacturing'],
      ['CEU3133400001', 'All Employees, Computer And Electronic Product Manufacturing NSA'],
      ['CES3133100001', 'All Employees, Primary Metal Manufacturing'],
      ['CEU3133100001', 'All Employees, Primary Metal Manufacturing NSA'],
      ['CES3133200001', 'All Employees, Fabricated Metal Product Manufacturing'],
      ['CEU3133200001', 'All Employees, Fabricated Metal Product Manufacturing NSA'],
      ['CES3133500001', 'All Employees, Electrical Equipment, Appliance, And Component Manufacturing'],
      ['CEU3133500001', 'All Employees, Electrical Equipment, Appliance, And Component Manufacturing NSA'],
      ['CES3133300001', 'All Employees, Machinery Manufacturing'],
      ['CEU3133300001', 'All Employees, Machinery Manufacturing NSA'],
      ['CES3133440001', 'All Employees, Semiconductor And Other Electronic Component Manufacturing'],
      ['CEU3133440001', 'All Employees, Semiconductor And Other Electronic Component Manufacturing NSA'],
      ['CES3133660001', 'All Employees, Ship and Boat Building'],
      ['CEU3133660001', 'All Employees, Ship and Boat Building NSA'],
      ['CES3133600001', 'All Employees, Transportation Equipment Manufacturing'],
      ['CEU3133600001', 'All Employees, Transportation Equipment Manufacturing NSA'],
      ['CES3133420001', 'All Employees, Communications Equipment Manufacturing'],
      ['CEU3133420001', 'All Employees, Communications Equipment Manufacturing NSA'],
      ['CES3133900001', 'All Employees, Miscellaneous Manufacturing'],
      ['CEU3133900001', 'All Employees, Miscellaneous Manufacturing NSA'],
      # Nondurable Goods
      ['NDMANEMP', 'All Employees, Nondurable Goods'],
      ['CEU3200000001', 'All Employees, Nondurable Goods NSA'],
      ['AWHAENDG', 'Average Weekly Hours of All Employees, Nondurable Goods'],
      ['CEU3200000002', 'Average Weekly Hours of All Employees, Nondurable Goods NSA'],
      ['CES3200000003', 'Average Hourly Earnings of All Employees, Nondurable Goods'],
      ['CEU3200000003', 'Average Hourly Earnings of All Employees, Nondurable Goods NSA'],
      ['CES3200000006', 'Production and Nonsupervisory Employees, Nondurable Goods'],
      ['CEU3200000006', 'Production and Nonsupervisory Employees, Nondurable Goods NSA'],
      ['CES3200000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Nondurable Goods'],
      ['CEU3200000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Nondurable Goods NSA'],
      ['CES3200000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Nondurable Goods'],
      ['CEU3200000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Nondurable Goods NSA'],
      ['CES3232500001', 'All Employees, Chemical Manufacturing'],
      ['CEU3232500001', 'All Employees, Chemical Manufacturing NSA'],
      ['CES3231100001', 'All Employees, Food Manufacturing'],
      ['CEU3231100001', 'All Employees, Food Manufacturing NSA'],
      ['CES3232400001', 'All Employees, Petroleum And Coal Products Manufacturing'],
      ['CEU3232400001', 'All Employees, Petroleum And Coal Products Manufacturing NSA'],
      ['CES3231500001', 'All Employees, Apparel Manufacturing'],
      ['CES3231500001', 'All Employees, Apparel Manufacturing NSA'],
      # Trade, Transportation, and Utilities
      ['USTPU', 'All Employees, Trade, Transportation, and Utilities'],
      ['CEU4000000001', 'All Employees, Trade, Transportation, and Utilities NSA'],
      ['AWHAETTU', 'Average Weekly Hours of All Employees, Trade, Transportation, and Utilities'],
      ['CEU4000000002', 'Average Weekly Hours of All Employees, Trade, Transportation, and Utilities NSA'],
      ['CES4000000003', 'Average Hourly Earnings of All Employees, Trade, Transportation, and Utilities'],
      ['CEU4000000003', 'Average Hourly Earnings of All Employees, Trade, Transportation, and Utilities NSA'],
      ['CES4000000006', 'Production and Nonsupervisory Employees, Trade, Transportation, and Utilities'],
      ['CEU4000000006', 'Production and Nonsupervisory Employees, Trade, Transportation, and Utilities NSA'],
      ['CES4000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Trade, Transportation, and Utilities'],
      ['CEU4000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Trade, Transportation, and Utilities NSA'],
      ['CES4000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Trade, Transportation, and Utilities'],
      ['CEU4000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Trade, Transportation, and Utilities NSA'],
      # Wholesale Trade
      ['USWTRADE', 'All Employees, Wholesale Trade'],
      ['CEU4142000001', 'All Employees, Wholesale Trade NSA'],
      ['AWHAEWT', 'Average Weekly Hours of All Employees, Wholesale Trade'],
      ['CEU4142000002', 'Average Weekly Hours of All Employees, Wholesale Trade NSA'],
      ['CES4142000003', 'Average Hourly Earnings of All Employees, Wholesale Trade'],
      ['CEU4142000003', 'Average Hourly Earnings of All Employees, Wholesale Trade NSA'],
      ['CES4142000006', 'Production and Nonsupervisory Employees, Wholesale Trade'],
      ['CEU4142000006', 'Production and Nonsupervisory Employees, Wholesale Trad NSA'],
      ['CES4142000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Wholesale Trade'],
      ['CEU4142000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Wholesale Trade NSA'],
      ['CES4142000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Wholesale Trade'],
      ['CEU4142000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Wholesale Trade NSA'],
      # Retail Trade
      ['USTRADE', 'All Employees, Retail Trade'],
      ['CEU4200000001', 'All Employees, Retail Trade NSA'],
      ['AWHAERT', 'Average Weekly Hours of All Employees, Retail Trade'],
      ['CEU4200000002', 'Average Weekly Hours of All Employees, Retail Trade NSA'],
      ['CES4200000003', 'Average Hourly Earnings of All Employees, Retail Trade'],
      ['CEU4200000003', 'Average Hourly Earnings of All Employees, Retail Trade NSA'],
      ['CES4200000006', 'Production and Nonsupervisory Employees, Retail Trade'],
      ['CEU4200000006', 'Production and Nonsupervisory Employees, Retail Trade'],
      ['CES4200000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Retail Trade'],
      ['CEU4200000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Retail Trade NSA'],
      ['CES4200000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Retail Trade'],
      ['CEU4200000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Retail Trade NSA'],
      ['CES4244800001', 'All Employees, Clothing, Clothing Accessories, Shoe, And Jewelry Retailers'],
      ['CEU4244800001', 'All Employees, Clothing, Clothing Accessories, Shoe, And Jewelry Retailers NSA'],
      ['CES4244100001', 'All Employees, Motor Vehicle and Parts Dealers'],
      ['CEU4244100001', 'All Employees, Motor Vehicle and Parts Dealers NSA'],
      ['CES4244700001', 'All Employees, Gasoline Stations And Fuel Dealers'],
      ['CEU4244700001', 'All Employees, Gasoline Stations And Fuel Dealers NSA'],
      ['CES4244500001', 'All Employees, Food And Beverage Retailers'],
      ['CEU4244500001', 'All Employees, Food And Beverage Retailers NSA'],
      ['CES4244600001', 'All Employees, Health And Personal Care Retailers'],
      ['CEU4244600001', 'All Employees, Health And Personal Care Retailers NSA'],
      ['CES4244300001', 'All Employees, Electronics And Appliance Retailers'],
      ['CEU4244300001', 'All Employees, Electronics And Appliance Retailers'],
      ['CES4245200001', 'All Employees, General Merchandise Retailers'],
      ['CEU4245200001', 'All Employees, General Merchandise Retailers'],
      ['CES4244200001', 'All Employees, Furniture And Home Furnishings Retailers'],
      ['CEU4244200001', 'All Employees, Furniture And Home Furnishings Retailers NSA'],
      ['CES4244400001', 'All Employees, Building Material And Garden Equipment And Supplies Dealers'],
      ['CEU4244400001', 'All Employees, Building Material And Garden Equipment And Supplies Dealers NSA'],
      # Transportation and Warehousing
      ['CES4300000001', 'All Employees, Transportation and Warehousing'],
      ['CEU4300000001', 'All Employees, Transportation and Warehousing NSA'],
      ['AWHAETAW', 'Average Weekly Hours of All Employees, Transportation and Warehousing'],
      ['CEU4300000002', 'Average Weekly Hours of All Employees, Transportation and Warehousing NSA'],
      ['CES4300000003', 'Average Hourly Earnings of All Employees, Transportation and Warehousing'],
      ['CEU4300000003', 'Average Hourly Earnings of All Employees, Transportation and Warehousing NSA'],
      ['CES4300000006', 'Production and Nonsupervisory Employees, Transportation and Warehousing'],
      ['CEU4300000006', 'Production and Nonsupervisory Employees, Transportation and Warehousing NSA'],
      ['CES4300000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Transportation and Warehousing'],
      ['CEU4300000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Transportation and Warehousing NSA'],
      ['CES4300000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Transportation and Warehousing'],
      ['CES4300000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Transportation and Warehousing NSA'],
      ['CES4348400001', 'All Employees, Truck Transportation'],
      ['CEU4348400001', 'All Employees, Truck Transportation NSA'],
      ['CES4348100001', 'All Employees, Air Transportation'],
      ['CEU4348100001', 'All Employees, Air Transportation NSA'],
      ['CES4349300001', 'All Employees, Warehousing and Storage'],
      ['CEU4349300001', 'All Employees, Warehousing and Storage NSA'],
      ['CES4349200001', 'All Employees, Couriers and Messengers'],
      ['CEU4349200001', 'All Employees, Couriers and Messengers NSA'],
      # Utilities
      ['CES4422000001', 'All Employees, Utilities'],
      ['CEU4422000001', 'All Employees, Utilities NSA'],
      ['AWHAEUTIL', 'Average Weekly Hours of All Employees, Utilities'],
      ['CEU4422000002', 'Average Weekly Hours of All Employees, Utilities'],
      ['CES4422000003', 'Average Hourly Earnings of All Employees, Utilities'],
      ['CEU4422000003', 'Average Hourly Earnings of All Employees, Utilities NSA'],
      ['CES4422000006', 'Production and Nonsupervisory Employees, Utilities'],
      ['CEU4422000006', 'Production and Nonsupervisory Employees, Utilities NSA'],
      ['CES4422000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Utilities'],
      ['CEU4422000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Utilities NSA'],
      ['CES4422000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Utilities'],
      ['CEU4422000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Utilities NSA'],
      # Information
      ['USINFO', 'All Employees, Information'],
      ['CEU5000000001', 'All Employees, Information NSA'],
      ['AWHAEINFO', 'Average Weekly Hours of All Employees, Information'],
      ['CEU5000000002', 'Average Weekly Hours of All Employees, Information NSA'],
      ['CES5000000003', 'Average Hourly Earnings of All Employees, Information'],
      ['CEU5000000003', 'Average Hourly Earnings of All Employees, Information NSA'],
      ['CES5000000006', 'Production and Nonsupervisory Employees, Information'],
      ['CEU5000000006', 'Production and Nonsupervisory Employees, Information NSA'],
      ['CES5000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Information'],
      ['CEU5000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Information NSA'],
      ['CES5000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Information'],
      ['CEU5000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Information NSA'],
      # Financial Activities
      ['USFIRE', 'All Employees, Financial Activities'],
      ['CEU5500000001', 'All Employees, Financial Activities NSA'],
      ['AWHAEFA', 'Average Weekly Hours of All Employees, Financial Activities'],
      ['CEU5500000002', 'Average Weekly Hours of All Employees, Financial Activities NSA'],
      ['CES5500000003', 'Average Hourly Earnings of All Employees, Financial Activities'],
      ['CEU5500000003', 'Average Hourly Earnings of All Employees, Financial Activities NSA'],
      ['CES5500000006', 'Production and Nonsupervisory Employees, Financial Activities'],
      ['CEU5500000006', 'Production and Nonsupervisory Employees, Financial Activities NSA'],
      ['CES5500000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Financial Activities'],
      ['CEU5500000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Financial Activities NSA'],
      ['CES5500000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Financial Activities'],
      ['CEU5500000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Financial Activities NSA'],
      ['CES5553100001', 'All Employees, Real Estate'],
      ['CEU5553100001', 'All Employees, Real Estate NSA'],
      ['CES5552000001', 'All Employees, Finance and Insurance'],
      ['CEU5552000001', 'All Employees, Finance and Insurance NSA'],
      ['CES5552211001', 'All Employees, Commercial Banking'],
      ['CEU5552211001', 'All Employees, Commercial Banking NSA'],
      ['CES5552300001', 'All Employees, Securities, Commodity Contracts, Funds, Trusts, And Other Financial Vehicles, Investments, And Related Activities'],
      ['CEU5552300001', 'All Employees, Securities, Commodity Contracts, Funds, Trusts, And Other Financial Vehicles, Investments, And Related Activities NSA'],
      ['CES5552200001', 'All Employees, Credit Intermediation and Related Activities'],
      ['CEU5552200001', 'All Employees, Credit Intermediation and Related Activities NSA'],
      # Professional and Business Services
      ['USPBS', 'All Employees, Professional and Business Services'],
      ['CEU6000000001', 'All Employees, Professional and Business Services NSA'],
      ['AWHAEPBS', 'Average Weekly Hours of All Employees, Professional and Business Services'],
      ['CEU6000000002', 'Average Weekly Hours of All Employees, Professional and Business Services NSA'],
      ['CES6000000003', 'Average Hourly Earnings of All Employees, Professional and Business Services'],
      ['CEU6000000003', 'Average Hourly Earnings of All Employees, Professional and Business Services NSA'],
      ['CES6000000006', 'Production and Nonsupervisory Employees, Professional and Business Services'],
      ['CEU6000000006', 'Production and Nonsupervisory Employees, Professional and Business Services NSA'],
      ['CES6000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Professional and Business Services'],
      ['CEU6000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Professional and Business Services NSA'],
      ['CES6000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Professional and Business Services'],
      ['CEU6000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Professional and Business Services NSA'],
      ['TEMPHELPS', 'All Employees, Temporary Help Services'],
      ['TEMPHELPN', 'All Employees, Temporary Help Services NSA'],
      ['CES6054000001', 'All Employees, Professional, Scientific, And Technical Services'],
      ['CEU6054000001', 'All Employees, Professional, Scientific, And Technical Services NSA'],
      # Education and Health Services
      ['USEHS', 'All Employees, Private Education And Health Services'],
      ['CEU6500000001', 'All Employees, Private Education And Health Services NSA'],
      ['AWHAEEHS', 'Average Weekly Hours of All Employees, Education and Health Services'],
      ['CEU6500000002', 'Average Weekly Hours of All Employees, Education and Health Services NSA'],
      ['CES6500000003', 'Average Hourly Earnings of All Employees, Education and Health Services'],
      ['CEU6500000003', 'Average Hourly Earnings of All Employees, Education and Health Services'],
      ['CES6500000006', 'Production and Nonsupervisory Employees, Education and Health Services'],
      ['CEU6500000006', 'Production and Nonsupervisory Employees, Education and Health Services NSA'],
      ['CES6500000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Education and Health Services'],
      ['CEU6500000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Education and Health Services NSA'],
      ['CES6500000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Education and Health Services'],
      ['CEU6500000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Education and Health Services NSA'],
      ['CES6562000101', 'All Employees, Health Care'],
      ['CEU6562000101', 'All Employees, Health Care NSA'],
      ['CES6562300001', 'All Employees, Nursing and Residential Care Facilities'],
      ['CEU6562300001', 'All Employees, Nursing and Residential Care Facilities NSA'],
      ['CES6562200001', 'All Employees, Hospitals'],
      ['CEU6562200001', 'All Employees, Hospitals NSA'],
      ['CES6562400001', 'All Employees, Social Assistance'],
      ['CEU6562400001', 'All Employees, Social Assistance NSA'],
      ['CES6561000001', 'All Employees, Private Educational Services'],
      ['CEU6561000001', 'All Employees, Private Educational Services NSA'],
      # Leisure and Hospitality
      ['USLAH', 'All Employees, Leisure and Hospitality'],
      ['CEU7000000001', 'All Employees, Leisure and Hospitality NSA'],
      ['AWHAELAH', 'Average Weekly Hours of All Employees, Leisure and Hospitality'],
      ['CEU7000000002', 'Average Weekly Hours of All Employees, Leisure and Hospitality NSA'],
      ['CES7000000003', 'Average Hourly Earnings of All Employees, Leisure and Hospitality'],
      ['CEU7000000003', 'Average Hourly Earnings of All Employees, Leisure and Hospitality NSA'],
      ['CES7000000006', 'Production and Nonsupervisory Employees, Leisure and Hospitality'],
      ['CEU7000000006', 'Production and Nonsupervisory Employees, Leisure and Hospitality NSA'],
      ['CES7000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Leisure and Hospitality'],
      ['CEU7000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Leisure and Hospitality NSA'],
      ['CES7000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Leisure and Hospitality'],
      ['CEU7000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Leisure and Hospitality NSA'],
      ['CES7072200001', 'All Employees, Food Services and Drinking Places'],
      ['CEU7072200001', 'All Employees, Food Services and Drinking Places NSA'],
      ['CES7071000001', 'All Employees, Arts, Entertainment, and Recreation'],
      ['CEU7071000001', 'All Employees, Arts, Entertainment, and Recreation NSA'],
      ['CES7071300001', 'All Employees, Amusement, Gambling, And Recreation Industries'],
      ['CEU7071300001', 'All Employees, Amusement, Gambling, And Recreation Industries NSA'],
      ['CES7071100001', 'All Employees, Performing Arts, Spectator Sports, And Related Industries'],
      ['CEU7071100001', 'All Employees, Performing Arts, Spectator Sports, And Related Industries NSA'],
      # Other Services
      ['USSERV', 'All Employees, Other Services'],
      ['CEU8000000001', 'All Employees, Other Services NSA'],
      ['AWHAEOS', 'Average Weekly Hours of All Employees, Other Services'],
      ['CEU8000000002', 'Average Weekly Hours of All Employees, Other Services NSA'],
      ['CES8000000003', 'Average Hourly Earnings of All Employees, Other Services'],
      ['CEU8000000003', 'Average Hourly Earnings of All Employees, Other Services'],
      ['CES8000000006', 'Production and Nonsupervisory Employees, Other Services'],
      ['CEU8000000006', 'Production and Nonsupervisory Employees, Other Services NSA'],
      ['CES8000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Other Services'],
      ['CEU8000000007', 'Average Weekly Hours of Production and Nonsupervisory Employees, Other Services NSA'],
      ['CES8000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Other Services'],
      ['CEU8000000008', 'Average Hourly Earnings of Production and Nonsupervisory Employees, Other Services NSA'],
      ['CES8081100001', 'All Employees, Repair and Maintenance'],
      ['CEU8081100001', 'All Employees, Repair and Maintenance NSA'],
      ['CES8081200001', 'All Employees, Personal and Laundry Services'],
      ['CEU8081200001', 'All Employees, Personal and Laundry Services NSA'],
      ['CES8081300001', 'All Employees, Religious, Grantmaking, Civic, Professional, And Similar Organizations'],
      ['CEU8081300001', 'All Employees, Religious, Grantmaking, Civic, Professional, And Similar Organizations NSA'],
      # Government
      ['USGOVT', 'All Employees, Government'],
      ['CEU9000000001', 'All Employees, Government NSA'],
      ['CES9091000001', 'All Emplo yees, Federal'],
      ['CEU9091000001', 'All Employees, Federal NSA'],
      ['CES9092000001', 'All Employees, State Government'],
      ['CEU9092000001', 'All Employees, State Government NSA'],
      ['CES9093000001', 'All Employees, Local Government'],
      ['CEU9093000001', 'All Employees, Local Government NSA'],


      ## ADP Employment
    #   ['ADPWNUSNERSA', 'Total Nonfarm Private Payroll Employment Weekly'],
    #   ['ADPWNUSNERNSA', 'Total Nonfarm Private Payroll Employment Weekly NSA'],
    #   ['ADPMNUSNERSA', 'Total Nonfarm Private Payroll Employment Monthly'],
    #   ['ADPMNUSNERNSA', 'Total Nonfarm Private Payroll Employment Monthly NSA'],
    #   # Manufacturing
    #   ['ADPWINDMANNERSA', 'Total Nonfarm Private Payroll Employment for Manufacturing Weekly'],
    #   ['ADPWINDMANNERNSA', 'Total Nonfarm Private Payroll Employment for Manufacturing Weekly NSA'],
    #   ['ADPMINDMANNERSA', 'Total Nonfarm Private Payroll Employment for Manufacturing Monthly'],
    #   ['ADPMINDMANNERNSA', 'Total Nonfarm Private Payroll Employment for Manufacturing Monthly NSA'],
    #   # Construction
    #   ['ADPWINDCONNERSA', 'Total Nonfarm Private Payroll Employment for Construction Weekly'],
    #   ['ADPWINDCONNERSA', 'Total Nonfarm Private Payroll Employment for Construction Weekly NSA'],
    #   ['ADPMINDCONNERSA', 'Total Nonfarm Private Payroll Employment for Construction Monthly'],
    #   ['ADPMINDCONNERNSA', 'Total Nonfarm Private Payroll Employment for Construction Monthly NSA'],
    #   # Professional and Business Services
    #   ['ADPWINDPROBUSNERSA', 'Total Nonfarm Private Payroll Employment for Professional and Business Services Weekly'],
    #   ['ADPWINDPROBUSNERNSA', 'Total Nonfarm Private Payroll Employment for Professional and Business Services Weekly NSA'],
    #   ['ADPMINDPROBUSNERSA', 'Total Nonfarm Private Payroll Employment for Professional and Business Services Monthly'],
    #   ['ADPMINDPROBUSNERNSA', 'Total Nonfarm Private Payroll Employment for Professional and Business Services Monthly NSA'],
    #   # Leisure and Hospitality
    #   ['ADPWINDLSHPNERSA', 'Total Nonfarm Private Payroll Employment for Leisure and Hospitality Weekly'],
    #   ['ADPWINDLSHPNERNSA', 'Total Nonfarm Private Payroll Employment for Leisure and Hospitality Weekly NSA'],
    #   ['ADPMINDLSHPNERSA', 'Total Nonfarm Private Payroll Employment for Leisure and Hospitality Monthly'],
    #   ['ADPMINDLSHPNERNSA', 'Total Nonfarm Private Payroll Employment for Leisure and Hospitality Monthly NSA'],
    #   # Information
    #   ['ADPWINDINFONERSA', 'Total Nonfarm Private Payroll Employment for Information Weekly'],
    #   ['ADPWINDINFONERNSA', 'Total Nonfarm Private Payroll Employment for Information Weekly NSA'],
    #   ['ADPMINDINFONERSA', 'Total Nonfarm Private Payroll Employment for Information Monthly'],
    #   ['ADPMINDINFONERNSA', 'Total Nonfarm Private Payroll Employment for Information Monthly NSA'],
    #   # Education and Health Services
    #   ['ADPWINDEDHLTNERSA', 'Total Nonfarm Private Payroll Employment for Education and Health Services Weekly'],
    #   ['ADPWINDEDHLTNERNSA', 'Total Nonfarm Private Payroll Employment for Education and Health Services Weekly NSA'],
    #   ['ADPMINDEDHLTNERSA', 'Total Nonfarm Private Payroll Employment for Education and Health Services Monthly'],
    #   ['ADPMINDEDHLTNERNSA', 'Total Nonfarm Private Payroll Employment for Education and Health Services Monthly NSA'],
    #   # Financial Activities
    #   ['ADPWINDFINNERSA', 'Total Nonfarm Private Payroll Employment for Financial Activities Weekly'],
    #   ['ADPWINDFINNERNSA', 'Total Nonfarm Private Payroll Employment for Financial Activities Weekly NSA'],
    #   ['ADPMINDFINNERSA', 'Total Nonfarm Private Payroll Employment for Financial Activities Monthly'],
    #   ['ADPMINDFINNERNSA', 'Total Nonfarm Private Payroll Employment for Financial Activities Monthly NSA'],
    #   # Trade Transportation and Utilities
    #   ['ADPWINDTTUNERSA', 'Total Nonfarm Private Payroll Employment for Trade Transportation and Utilities Weekly'],
    #   ['ADPWINDTTUNERNSA', 'Total Nonfarm Private Payroll Employment for Trade Transportation and Utilities Weekly NSA'],
    #   ['ADPMINDTTUNERSA', 'Total Nonfarm Private Payroll Employment for Trade Transportation and Utilities Monthly'],
    #   ['ADPMINDTTUNERNSA', 'Total Nonfarm Private Payroll Employment for Trade Transportation and Utilities Monthly NSA'],
    #   # Other Services
    #   ['ADPWINDOTHSRVNERSA', 'Total Nonfarm Private Payroll Employment for Other Services Weekly'],
    #   ['ADPWINDOTHSRVNERNSA', 'Total Nonfarm Private Payroll Employment for Other Services Weekly NSA'],
    #   ['ADPMINDOTHSRVNERSA', 'Total Nonfarm Private Payroll Employment for Other Services Monthly'],
    #   ['ADPMINDOTHSRVNERNSA', 'Total Nonfarm Private Payroll Employment for Other Services Monthly NSA'],
    #   # Natural Resources and Mining
    #   ['ADPWINDNRMINNERSA', 'Total Nonfarm Private Payroll Employment for Natural Resources and Mining Weekly'],
    #   ['ADPWINDNRMINNERNSA', 'Total Nonfarm Private Payroll Employment for Natural Resources and Mining Weekly NSA'],
    #   ['ADPMINDNRMINNERSA', 'Total Nonfarm Private Payroll Employment for Natural Resources and Mining Monthly'],
    #   ['ADPMINDNRMINNERNSA', 'Total Nonfarm Private Payroll Employment for Natural Resources and Mining Monthly NSA'],
    ]



    #    ['CCSA', 'Continued Claims'],
    #    ['CC4WSA', '4-Week Moving Average of Continued Claims'],
    #    ['CCNSA', 'Continued Claims NSA'],
    #    ['UEMPMEAN', 'Average Weeks Unemployed'],


def get_fred_unemployment_data():
    s = {}
    for i in range(len(ids)):
        while(1):
            try:
                s[ids[i][1]] = fred.get_series(ids[i][0], observation_start='2000-01-01')
                print(ids[i][1])
                break
            except:
                print('exception: '+str(i)+', '+ids[i][1]+'-----------')
                break

    df = pd.DataFrame(s)
    df.rename_axis("time", inplace=True)
    path = os.path.join(data_dir, 'unemployment'+'.csv')
    df.to_csv(path, encoding='utf-8')

def update_fred_unemployment_data():
    path = os.path.join(data_dir, 'unemployment'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    observation_start_old = (t[-1] - pd.Timedelta(days = 420)).strftime('%Y-%m-%d')
    observation_start_new = '2000-01-01'
    columns = df.columns.tolist()

    s_old = {}
    s_new = {}
    for i in range(len(ids)):
        while(1):
            try:
                if (ids[i][1] in columns):
                    s_old[ids[i][1]] = fred.get_series(ids[i][0], observation_start=observation_start_old)
                    print(ids[i][1], observation_start_old)
                    break
                else:
                    s_new[ids[i][1]] = fred.get_series(ids[i][0], observation_start=observation_start_new)
                    print(ids[i][1], observation_start_new)
                    break       
            except Exception as e:
                print('exception: '+ e + ', '+ids[i][1]+'-----------')
                break


    df_old = pd.DataFrame(s_old)
    df_old.reset_index(inplace=True)
    df_old.rename(columns={'index':'time'}, inplace=True)
    df_old['time'] = df_old['time'].dt.strftime('%Y-%m-%d')

    df = pd.concat([df, df_old], axis=0)
    df.drop_duplicates('time', keep='last', inplace=True)

    df_new = pd.DataFrame(s_new)
    df_new.reset_index(inplace=True)
    df_new.rename(columns={'index':'time'}, inplace=True)
    df_new['time'] = df_new['time'].dt.strftime('%Y-%m-%d')

    df = pd.merge(df, df_new, on='time', how='outer')

    path = os.path.join(data_dir, 'unemployment'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False)


# 不同行业的就业人数
def test1():
    industry = ['All Employees, Total Nonfarm',
                'All Employees, Goods-Producing',
                'All Employees, Private Service-Providing',
                'All Employees, Mining and Logging',
                'All Employees, Construction',
                'All Employees, Manufacturing',
                'All Employees, Durable Goods',
                'All Employees, Nondurable Goods',
                'All Employees, Wholesale Trade',
                'All Employees, Retail Trade',
                'All Employees, Transportation and Warehousing',
                'All Employees, Utilities',
                'All Employees, Information',
                'All Employees, Financial Activities',
                'All Employees, Professional and Business Services',
                'All Employees, Private Education And Health Services',
                'All Employees, Leisure and Hospitality',
                'All Employees, Other Services',
                'All Employees, Government',]
    
    path = os.path.join(data_dir, 'unemployment'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    datas = []
    latest = []
    for i in range(len(industry)):
        datas.append([t, np.array(df[industry[i]], dtype=float), industry[i]])
        latest.append(datas[i][1][-1])
    
    latest = np.array(latest, dtype=float)
    order = np.argsort(latest)[::-1]
    datas_order = [datas[i] for i in order]
    plot_one_figure(datas_order, '就业人数', '2000-01-01', '2100-01-01')


# 不同行业的失业率
def test2():
    industry = ['Unemployment Rate - Construction Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Manufacturing Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Agricultural and Related Private Wage and Salary Workers NSA',
                'Unemployment Rate - Mining, Quarrying, and Oil and Gas Extraction, Nonagricultural Private Wage and Salary Workers NSA',
                'Unemployment Rate - Wholesale and Retail Trade, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Leisure and Hospitality, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Education and Health Services, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Financial Activities Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Management, Business, and Financial Operations Occupations NSA',
                'Unemployment Rate - Transportation and Utilities Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Information Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Professional and Business Services Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - Durable Goods Industry, Private Wage and Salary Workers NSA',
                'Unemployment Rate - All Industries, Self-Employed, Unincorporated, and Unpaid Family Workers NSA',
                'Unemployment Rate All Industries Government Wage & Salary Workers NSA',
                'Unemployment Rate Part-Time Workers',]
    
    path = os.path.join(data_dir, 'unemployment'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    datas = []
    latest = []
    for i in range(len(industry)):
        datas.append([t, np.array(df[industry[i]], dtype=float), industry[i]])
        latest.append(datas[i][1][-1])
    
    latest = np.array(latest, dtype=float)
    order = np.argsort(latest)[::-1]
    datas_order = [datas[i] for i in order]
    plot_one_figure(datas_order, '失业率', '2000-01-01', '2100-01-01')

# 不同行业的时薪
def test3():
    industry = ['Average Hourly Earnings of All Employees, Total Private',
                'Average Hourly Earnings of All Employees, Goods-Producing',
                'Average Hourly Earnings of All Employees, Private Service-Providing',
                'Average Hourly Earnings of All Employees, Mining and Logging',
                'Average Hourly Earnings of All Employees, Construction',
                'Average Hourly Earnings of All Employees, Manufacturing',
                'Average Hourly Earnings of All Employees, Durable Goods',
                'Average Hourly Earnings of All Employees, Nondurable Goods',
                'Average Hourly Earnings of All Employees, Trade, Transportation, and Utilities',
                'Average Hourly Earnings of All Employees, Wholesale Trade',
                'Average Hourly Earnings of All Employees, Retail Trade',
                'Average Hourly Earnings of All Employees, Transportation and Warehousing',
                'Average Hourly Earnings of All Employees, Utilities',
                'Average Hourly Earnings of All Employees, Information',
                'Average Hourly Earnings of All Employees, Financial Activities',
                'Average Hourly Earnings of All Employees, Professional and Business Services',
                'Average Hourly Earnings of All Employees, Education and Health Services',
                'Average Hourly Earnings of All Employees, Leisure and Hospitality',
                'Average Hourly Earnings of All Employees, Other Services',]
    
    path = os.path.join(data_dir, 'unemployment'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    datas = []
    earnings = []
    latest = []
    for i in range(len(industry)):
        earnings.append(np.array(df[industry[i]], dtype=float))
        datas.append([t, earnings[i], industry[i]])
        latest.append(datas[i][1][-1])
    
    latest = np.array(latest, dtype=float)
    order = np.argsort(latest)[::-1]
    datas_order = [datas[i] for i in order]
    plot_one_figure(datas_order, '平均时薪', '2000-01-01', '2100-01-01')


    datas_yoy = []
    latest = []
    for i in range(len(industry)):
        ret = yoy_for_monthly_data(t, earnings[i])
        datas_yoy.append([ret[0], ret[1], industry[i]])
        latest.append(datas_yoy[i][1][-1])

    latest = np.array(latest, dtype=float)
    order = np.argsort(latest)[::-1]
    datas_yoy_order = [datas_yoy[i] for i in order]
    plot_one_figure(datas_yoy_order, '平均时薪 同比', '2000-01-01', '2100-01-01')

# native vs foreign born
def test4():
    path = os.path.join(data_dir, 'unemployment'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))


    native_unrate = np.array(df['Unemployment Rate - Native Born NSA'], dtype=float)
    foreign_unrate = np.array(df['Unemployment Rate - Foreign Born NSA'], dtype=float)

    native_clf = np.array(df['Civilian Labor Force Level - Native Born'], dtype=float)
    foreign_clf = np.array(df['Civilian Labor Force Level - Foreign Born'], dtype=float)

    native_part = np.array(df['Labor Force Participation Rate - Native Born'], dtype=float)
    foreign_part = np.array(df['Labor Force Participation Rate - Foreign Born'], dtype=float)

    native_eml = np.array(df['Employment Level - Native Born'], dtype=float)
    foreign_eml = np.array(df['Employment Level - Foreign Born'], dtype=float)

    native_emratio = np.array(df['Employment-Population Ratio - Native Born'], dtype=float)
    foreign_emratio = np.array(df['Employment-Population Ratio - Foreign Born'], dtype=float)

    datas = [
             [[[t,native_unrate,'Unemployment Rate - Native Born NSA',''],[t,foreign_unrate,'Unemployment Rate - Foreign Born NSA','']],[],''],
             [[[t,native_part,'Labor Force Participation Rate - Native Born',''],[t,foreign_part,'Labor Force Participation Rate - Foreign Born','']],[],''],
             [[[t,native_clf,'Civilian Labor Force Level - Native Born','']],[[t,foreign_clf,'Civilian Labor Force Level - Foreign Born','']],''],
             [[[t,native_eml,'Employment Level - Native Born','']],[[t,foreign_eml,'Employment Level - Foreign Born','']],''],
            ]
    plot_many_figure(datas, '2000-01-01', '2100-01-01')


# get_fred_unemployment_data()
update_fred_unemployment_data()

# # 不同行业的就业人数
# test1()

# # 不同行业的失业率
# test2()

# # 不同行业的时薪
# test3()

# # native vs foreign born
# test4()
