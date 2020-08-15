from main import ETL

def DownloadingAttachemntTest(): 
    a = ETL()
    assert isinstance(a.DownloadingEmailAttachment('Mukiza Rayner', 'HELLO TEST MAIL'), str)