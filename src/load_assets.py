"""
Module for Loading and Cleaning WRDS Reports

This module provides utility functions to load and clean data extracted from WRDS (Wharton Research Data Services)
reports. It supports reading CSV files from a specified directory and cleaning different types of financial datasets,
including assets, loans, and other related data.

Modules/Packages:
    - pandas: Used for data manipulation and analysis.
    - load_WRDS: Custom module for loading WRDS-related data.
    - config: Provides configuration settings, including the DATA_DIR.
    - data_read: Utility module for additional data reading functions.

Functions:
    - load_wrds_reports(file_name):
          Loads a WRDS report CSV file from the manual subdirectory in the configured data directory.

    - clean_assets(df, asset_col, date):
          Cleans the assets DataFrame by selecting specific columns, dropping missing values, renaming columns,
          and filtering the data based on the provided report date.

    - clean_loans(df, loan_cols, date):
          Cleans the loans DataFrame by selecting relevant columns (including a list of loan-specific columns),
          dropping missing values, renaming columns to standard labels, and filtering by the specified report date.

    - clean_others(df, other_cols, other_names, date):
          Cleans other financial DataFrames by selecting the base columns along with additional specified columns,
          dropping missing values, renaming columns according to provided names, and filtering by report date.
"""

import pandas as pd
import load_WRDS
import config
import data_read

DATA_DIR = config.DATA_DIR

def load_wrds_reports(file_name):
    """
    Load the WRDS reports
    """
    path = (DATA_DIR / 'manual' / file_name)
    df = pd.read_csv(f'{path}.csv')
    return df

def clean_assets(df,asset_col,date):
    """
    Clean the assets dataframe
    """
    col_ls = ['RSSD9001','RSSD9017','RSSD9999']
    col_ls.append(asset_col)
    df = df[col_ls]
    df = df.dropna()
    df.columns = ['Bank_ID','bank_name','report_date','gross_asset']
    df = df[df['report_date'] == date]  


    return df 

def clean_loans(df,loan_cols,date):
    """
    Clean the loans dataframe
    """
    col_ls = ['RSSD9001','RSSD9017','RSSD9999']
    col_ls = col_ls + loan_cols
    df = df[col_ls]
    df = df.dropna()
    df.columns = ['Bank_ID','bank_name','report_date','<3m', '3m-1y','1y-3y','3y-5y','5y-15y','>15y']
    df = df[df['report_date'] == date]  

    return df 

def clean_others(df,other_cols,other_names,date):
    """
    Clean the other dataframes
    """
    col_ls = ['RSSD9001','RSSD9017','RSSD9999']
    col_ls = col_ls + other_cols
    df = df[col_ls]
    df = df.dropna()
    df.columns = ['Bank_ID','bank_name','report_date']+other_names
    df = df[df['report_date'] == date]  

    return df 
