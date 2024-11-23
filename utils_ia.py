import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential

from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, AnalyzeResult, DocumentAnalysisFeature

load_dotenv()

endpoint = os.environ["AISERVICES_ENDPOINT"]
key = os.environ["AISERVICES_KEY"]

def capture_text_from_image(image_url: str):     

    client = ImageAnalysisClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key)
    )

    try:
        result = client.analyze_from_url(
            image_url=image_url,
            visual_features=[VisualFeatures.CAPTION, VisualFeatures.READ],
            gender_neutral_caption=True
        )
    except Exception as e:
        if "InvalidImageSize" in str(e):
            return {"error": "InvalidImageSize"}

    # Capture caption and confidence from the image
    caption = ""
    confidence = ""
    if result.caption is not None:
        caption = result.caption.text
        confidence = result.caption.confidence

    # Capture all texts from the image
    fulltext = ''
    if result.read is not None:
        for line in result.read.blocks[0].lines:
            fulltext += str(line.text) + " "

    result_json = {
        "caption": caption,
        "confidence": confidence,
        "fulltext": fulltext
    }

    return result_json

def capture_text_from_pdf(blob_url: str):

    document_intelligence_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key), api_version="2024-02-29-preview"
    )

    poller = document_intelligence_client.begin_analyze_document(
        "prebuilt-layout", 
        AnalyzeDocumentRequest(url_source=blob_url),
        features=["keyValuePairs"]
    )

    result: AnalyzeResult = poller.result()

    form_fields = {}
    try:
        if result.content:
            content = result.content.replace("\n", " ")
        
        for pair in result.key_value_pairs:

            if pair.key and pair.key.content:
                key_content = pair.key.content.replace("\n", "\\n").replace("/", "\/")
                    
            if pair.value and pair.value.content:
                value_content = pair.value.content.replace("\n", "\\n").replace("/", "\/")
            else:
                value_content = ""

            # add to form_fields json
            form_fields.update({key_content : value_content})       

        # get data from tables present in the pdf
        headers = []
        row_json = {}
        table_json = {}
        tables_array = []
        for table in result.tables:
            for cell in table.cells:
                row_index = cell.row_index
                if cell.row_index == 0:
                    # append cell content to headers list
                    headers.append(cell.content)
                else:
                    item_json = {headers[cell.column_index] : cell.content}
                    row_json.update(item_json)
                    if cell.column_index == len(headers) - 1:
                        table_json.update({row_index : row_json})
                        row_json = {}
            tables_array.append(table_json)
            # clean headers and table_json
            headers = []
            table_json = {}
        
    except Exception as e:
        print(str(e))

    result_json = {
        "content": content,
        "fields": form_fields,
        "tables": tables_array
    }

    return result_json

def capture_text_from_office(blob_url: str):

    try:
        document_intelligence_client = DocumentIntelligenceClient(
            endpoint=endpoint, credential=AzureKeyCredential(key), api_version="2024-02-29-preview"
        )

        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-layout", 
            AnalyzeDocumentRequest(url_source=blob_url)
        )

        result: AnalyzeResult = poller.result()

        result_json = {
            "content": result.content.replace("\n", " ")
        }

        return result_json
    
    except Exception as e:
        return {"error": str(e)}
