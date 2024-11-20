from dagster import ConfigurableResource,AssetExecutionContext
import os
import httpx


class ParquetDownloadResource(ConfigurableResource):
    base_url: str
    years: list[int]
    months: list[int]
    download_folder: str

    def ensure_download_folder_exists(self, context:AssetExecutionContext):
        """
        Ensures that the download folder exists. 
        If the folder does not exist, it is created.

        Args:
            context: Dagster context for logging.
        """
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder, exist_ok=True)
            context.log.info(f"Created download folder at {self.download_folder}")
        else:
            context.log.info(f"Download folder already exists at {self.download_folder}")

    def download_parquet_data(self, context:AssetExecutionContext):
        """
        Downloads Parquet files from a specified URL for each year and month combination.

        Args:
            context: Dagster context for logging.
        """
        #  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet_2023-03.parquet
        # base_filename = os.path.basename(self.base_url).split('_')[0] + '_' + self.base_url.split('_')[1]
        base_filename = os.path.basename(self.base_url).split('/')[-1].split('_')[0] + '_' + self.base_url.split('/')[-1].split('_')[1]
        for year in self.years:
            for month in self.months:
                modified_url = f"{self.base_url.split('_')[0] + '_' + self.base_url.split('_')[1]}_{year}-{month:02d}.parquet"
                filename = f"{base_filename}_{year}-{month:02d}.parquet"
                file_path = os.path.join(self.download_folder, filename)

                context.log.info(f"Preparing to download file: {file_path}")
                context.log.info(f"Modified_url: {modified_url}")
                # try:
                with httpx.Client() as client:
                    response = client.get(modified_url)
                    if response.status_code == 200:
                        with open(file_path, "wb") as file:
                            file.write(response.content)
                        context.log.info(f"Download complete: {filename}")
                    else:
                        context.log.error(
                            f"Failed to download {filename}. Status code: {response.status_code}"
                        )
                # except Exception as e:
                #     context.log.error(f"Error during download: {e}")
