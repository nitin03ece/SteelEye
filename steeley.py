import os
import re
import csv
import boto3
import logging
import requests
import boto3.s3
import traceback
from zipfile import ZipFile
import xml.etree.ElementTree as ET



def startLogging():
    """Initialize basic configuration for logging"""
    logging.basicConfig(
        filename="steeleye.txt",
        filemode='w',
        format='[%(levelname)s] [%(filename)s:%(lineno)d] : %(message)s',
        level=logging.INFO
    )


def get_root(file):
    """Return the root element of the XML File"""
    try:
        tree = ET.parse(file)
        root = tree.getroot()
    except Exception as e:
        error_info = f"{str(e)} {traceback.extract_stack()}"
        logging.error(error_info)
        return None
    else:
        return root


def download_zip_file(root):
    """Return the root element of the XML File"""
    download_zip = None
    for item in root.iter('str'):
        if (item.attrib['name'] == 'download_link' and 
            'DLTINS' in item.text):
            download_zip = item.text
            break
            
    req = requests.get(download_zip)
    file_name = "steeley.zip"

    try:
        with open(file_name, 'wb') as file:
            file.write(req.content)
    except Exception as e:
        error_info = f"{str(e)} {traceback.extract_stack()}"
        logging.error(error_info)
        return None
    else:
        pass

    try:
        with ZipFile(file_name, 'r') as file:
            file.printdir()
            file.extractall()
    except Exception as e:
        error_info = f"{str(e)} {traceback.extract_stack()}"
        logging.error(error_info)
        return None
    else:
        xml_file = 'DLTINS_20210117_01of01.xml'
        return xml_file


def get_csv_file(root):
    id_index = 0
    full_nm_index = 1
    clssfctn_tp_index = 2
    cmmdty_deriv_ind_index = 3
    ntnl_ccy_index = 4
    issr_index = 5
    csv_file_name = 'steeley.csv'
    header = [
        'FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 
        'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr'
        ]
    TOTAL_HEADER = len(header)

    try:
        with open(csv_file_name, 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for item in root.iter():
                if "FinInstrmGnlAttrbts" in item.tag:
                    data = [None] * TOTAL_HEADER
                    for child in item:
                        if "Id" in child.tag:
                            data[id_index] = child.text if re.search("Id$", child.tag) is not None else ""

                        elif "FullNm" in child.tag:
                            data[full_nm_index] = child.text if re.search("FullNm$", child.tag) is not None else ""

                        elif "ClssfctnTp" in child.tag:
                            data[clssfctn_tp_index] = child.text if re.search("ClssfctnTp$", child.tag) is not None else ""

                        elif "CmmdtyDerivInd" in child.tag:
                            data[cmmdty_deriv_ind_index] = child.text if re.search("CmmdtyDerivInd$", child.tag) is not None else ""

                        elif "NtnlCcy" in child.tag:
                            data[ntnl_ccy_index] = child.text if re.search("NtnlCcy$", child.tag) is not None else ""
                            
                elif re.search("Issr$", item.tag):
                    data[issr_index] = item.text if re.search("Issr$", item.tag) is not None else ""
                    writer.writerow(data)
    except Exception as e:
        error_info = f"{str(e)} {traceback.extract_stack()}"
        logging.error(error_info)
        if not os.path.exists(csv_file_name):
            return None
    else:
        return csv_file_name


def main(xml_file):
    root_one = get_root(xml_file)
    if root_one:
        xml_file_two = download_zip_file(root_one)
        if xml_file_two:       
            root_two = get_root(xml_file_two)
            if root_two:
                csv_file_name = get_csv_file(root_two)
                if csv_file_name:
                    try:
                        s3 = boto3.client('s3')
                        bucketName = "steeleye03"
                        bucket = s3.create_bucket(Bucket=bucketName)
                        s3.upload_file(csv_file_name, bucketName, "s3_"+csv_file_name)
                    except Exception as e:
                        error_info = f"{str(e)} {traceback.extract_stack()}"
                        logging.error(error_info)
                else:
                    log_info = f"{csv_file_name}] Does not exist!"
                    logging.info(log_info)
            else:
                log_info = f"Existing root_two variable : [{root_two}] is not valid"
                logging.info(log_info)
        else:
            log_info = f"Existing xml_file variable : [{xml_file}] is not valid"
            logging.info(log_info)
    else:
        log_info = f"Existing root_one variable : [{root_one}] is not valid"
        logging.info(log_info)


if __name__ == "__main__":
    startLogging()
    xml_file = 'steeley.xml'
    main(xml_file)