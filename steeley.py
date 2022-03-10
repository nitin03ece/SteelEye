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
    """Initialize basic configuration for logging."""
    logging.basicConfig(
        filename="steeleye.txt",
        filemode='w',
        format='[%(levelname)s] [%(filename)s:%(lineno)d] : %(message)s',
        level=logging.INFO
    )

class FileConversion:
    """File Conversion class. This deals with many file related functionality namely:
        - Get root element of the xml file.
        - Search zip file, within the xml content and dowload to the current directory
        - Extract the downloaded zip file.
        - Convert the extracted file into csv format.
    """
    def get_xml_root(file):
        """This Function takes the XML file as an argument and returns its root element."""
        try:
            tree = ET.parse(file)
            root = tree.getroot()
        except Exception as e:
            error_info = f"{str(e)} {traceback.extract_stack()}"
            logging.error(error_info)
            return None
        else:
            return root


    def download_zip_file(root, download_link_type="DLTINS"):
        """This Function takes root element(xml file) and a substring of download link as an argument. 
        Searches for the first download link matching that substring and 
        download that zip file and perform its extraction.
        """
        download_zip = None
        for item in root.iter('str'):
            if (item.attrib['name'] == download_link_type and 
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
                file.extractall()
        except Exception as e:
            error_info = f"{str(e)} {traceback.extract_stack()}"
            logging.error(error_info)
            return None
        else:
            xml_file = 'DLTINS_20210117_01of01.xml'
            return xml_file


    def convert_xml_to_csv(root):
        """This Function takes root element(xml file) as an argument. 
        Parse through the xml content and filters out specific subtags under 'FinInstrmGnlAttrbts' tag such as:
            - Id
            - FullNm
            - ClssfctnTp
            - CmmdtyDerivInd
            - NtnlCcy
            - Issr
        Parsed filtered data is further written in csv file. This function returns the created csv file.
        """
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


class AWSUpload:
    """AWS Upload Class. This class uploads file in the given AWS S3 bucket"""
    def __init__(self, bucket):
        """AWSUpload Class init function. Takes existing S3 bucket as an argument."""
        self.s3_client = boto3.client('s3')
        self.s3_bucket = bucket


    def upload_file(self, file_to_upload, file_upload_name):
        """This function take file and file name string as an argument.
        And uploads the file in the given S3 bucket.
        """
        try:
            self.s3_client.upload_file(file_to_upload, self.s3_bucket, file_upload_name)
        except Exception as e:
            error_info = f"{str(e)} {traceback.extract_stack()}"
            logging.error(error_info)
        else:
            pass


def main(xml_file):
    """Python main function, which take an xml file as an argument 
    and extract desirable infomation 
    and upload that data to AWS S3 bucket.
    """
    file = FileConversion()
    root_one = file.get_xml_root(xml_file)
    if root_one:
        xml_file_two = file.download_zip_file(root_one)
        if xml_file_two:       
            root_two = file.get_xml_root(xml_file_two)
            if root_two:
                csv_file_name = file.convert_xml_to_csv(root_two)
                if csv_file_name:
                    bucketName = "steeleye03"
                    aws_s3 = AWSUpload(bucketName)
                    aws_s3.upload_file(csv_file_name, "s3_"+csv_file_name)
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