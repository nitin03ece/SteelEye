from steeleye import *
import os
import unittest

class TestFileConversion(unittest.TestCase):
    def test_invalid_xml_file(self):
        file = FileConversion()
        text_file = "steeleye.txt"
        # text_file = "steeleye.csv"
        # text_file = "steeleye.html"
        root = file.get_xml_root(text_file)
        self.assertEqual(root, None)


    def test_valid_xml_file(self):
        file = FileConversion()
        file_name = "steeleye.xml"
        root = file.get_xml_root(file_name)
        self.assertNotEqual(root, None)


    def test_Zip_file_downloaded(self):
        file = FileConversion()
        file_name = "steeleye.xml"
        root = file.get_xml_root(file_name)
        zip_file = file.download_zip_file(root)
        self.assertEqual(os.path.exists(zip_file), True)


    def test_Zip_file_extracted(self):
        file = FileConversion()
        file_name = "steeleye.xml"
        root = file.get_xml_root(file_name)
        zip_file = file.download_zip_file(root)
        extracted_file = file.extract_zip_file(zip_file)
        expected_extracted_file = "DLTINS_20210117_01of01.xml"
        self.assertEqual(expected_extracted_file, extracted_file)


    def test_csv_file_generated(self):
        csv_file_name = 'steeleye.csv'
        file = FileConversion()
        file_name = "steeleye.xml"
        root = file.get_xml_root(file_name)
        zip_file = file.download_zip_file(root)
        extracted_file = file.extract_zip_file(zip_file)
        root_2 = file.get_xml_root(extracted_file)
        csv_file = file.convert_xml_to_csv(root_2)
        self.assertEqual(csv_file_name, csv_file)


if __name__ == "__main__":
    unittest.main()