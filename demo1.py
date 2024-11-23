import logging
from utils_lake import *
from utils_ia import *

CONTAINER_DOCS = os.environ['CONTAINER_DOCS']
CONTAINER_JSONS = os.environ['CONTAINER_JSONS']

def process01():
    try:       

        directory = ''

        files = list_files(CONTAINER_DOCS, directory)

        logging.info(f'Files listed: {files}')

        print(f'Files listed: {files}')

        processed_files = []
        format_not_supported_files = []

        for file in files:

            logging.info(f'Starting processing: {file}')

            # image
            if file.endswith(".jpeg") or file.endswith(".jpg") or file.endswith(".png"):
                blob_path = get_filepath_from_lake(CONTAINER_DOCS, file)
                result = capture_text_from_image(blob_path)
                
            # pdf
            elif file.endswith(".pdf"):
                blob_path = get_filepath_from_lake(CONTAINER_DOCS, file)
                result = capture_text_from_pdf(blob_path)

            # office files
            elif file.endswith(".docx") or file.endswith(".xlsx") or file.endswith(".pptx"):
                blob_path = get_filepath_from_lake(CONTAINER_DOCS, file)
                result = capture_text_from_office(blob_path)
            
            else:
                format_not_supported_files.append(file)
                logging.info(f'File format not supported: {file}')
                print(f'File format not supported: {file}')
                continue

            save_json_to_lake(CONTAINER_DOCS, file, result)

            logging.info(f'File processed: {file}')

            print(f'File processed: {file}')

            # append file to processed_files
            processed_files.append(file)

        logging.info('All files saved as json and context obtained.\n')

        logging.info(f'\n Processing complete! Files processed: {processed_files}. \n\n Not supported (skipped) files: {format_not_supported_files}\n')

    except Exception as e:
        print(str(e))

__main__ = process01()