import asyncio
import aiohttp
import aiofiles
import os
import sys
import json
from pathlib import Path
from tqdm.asyncio import tqdm
import logging
from typing import List, Dict


logger = logging.getLogger(__name__)


def download_files(urls: List[str], folder: Path, max_connections: int = 50, chunk_size: int = 50_000_000):
    """download all files from urls"""
    semaphore = asyncio.BoundedSemaphore(max_connections)

    async def download_file(url: str):
        filename = url.split("/")[-1]
        filepath = folder / filename
        # fetch data from url
        async with semaphore, aiohttp.ClientSession() as session:
            logger.info(f"started session to download {filename} from {url}")
            async with session.get(url) as response:
                try:
                    assert response.status == 200
                except AssertionError:
                    logger.error(
                        f"got response status {response.status} in attempt to "
                        f"download {filename} from {url}"
                    )
                    raise
                
                logger.info(f"response status ok, start downloading {filename} to {filepath}")
                async with aiofiles.open(filepath, mode="wb") as outfile:
                    async for data in tqdm(response.content.iter_chunked(chunk_size), desc=f"{filepath}: "):
                        await outfile.write(data)

        logger.info(f"{filepath} done")
        
    # build event loop
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(download_file(url)) for url in urls]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


def download_ntuples(ntuples: Dict, folder: Path):
    """
    assumes ntuples to be 
    {
        sample: {
            variation: {
                files: [
                    {path: url1, ...},
                    {path: url2, ...},
                    ...
                ]
            }
        }
    }
    and downloads all urls, creating the directory structure sample/variation/{file1,file2,...}
    """
    for sample in ntuples:
        for variation in ntuples[sample]:
            outputfolder = folder / sample / variation
            os.makedirs(outputfolder, exist_ok=True)
            urls = [file["path"] for file in ntuples[sample][variation]["files"]]
            logger.info(f"start downloading data into {sample}/{variation}")
            download_files(urls, outputfolder)
            logger.info(f"done downloading into {sample}/{variation}")


def main(ntuples_list: str, outputfolder: str):
    with open(ntuples_list) as f:
        ntuples = json.load(f)
    
    download_ntuples(ntuples, Path(outputfolder))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format='%(levelname)-8s %(message)s')
    ntuples_list = sys.argv[1]
    outputfolder = sys.argv[2]
    main(ntuples_list, outputfolder)
