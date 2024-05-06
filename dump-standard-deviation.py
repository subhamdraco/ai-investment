import gspread
import json
from tqdm import tqdm
import time
import logging


class DumpStd:

    def __init__(self):
        self.gc = gspread.service_account(filename='keys.json')
        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)
        self.final_data = []
        logging.basicConfig(
            level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename="app.log",  # Log messages will be written to this file
            filemode="w"  # 'w' for overwrite, 'a' for append
        )

    def connect_to_gs(self, gs_name):
        return self.gc.open_by_url(gs_name)

    def get_stocks(self):
        global_index = self.connect_to_gs(self.config['global_index'])
        worksheet = global_index.worksheet('Supporting Data')
        stock_list = worksheet.get('B4:C1088')
        return stock_list

    def connect_to_historical(self, hist_sheet, worksheet_name):
        sheet = self.connect_to_gs(hist_sheet)
        worksheet = sheet.worksheet(worksheet_name)
        return worksheet

    def get_std_data_and_update(self):

        stock_with_index = self.get_stocks()
        global_index = self.connect_to_gs(self.config['global_index'])
        global_worksheet = global_index.worksheet('Supporting Data')
        # currently support NYSE , NASDAQ and JKSE
        for stock_data in tqdm(stock_with_index, desc='Collecting Data..'):
            try:
                index = stock_data[0]
                stock = stock_data[1].replace('.JK','')
                if index not in ['JKSE', 'NASDAQ', 'NYSE']:
                    logging.info(f'No URL found for {stock}')
                    continue
                portfolio_sheet = self.connect_to_gs(self.config[index])
                portfolio_worksheet = portfolio_sheet.worksheet('Config')
                stock_row_p = portfolio_worksheet.find(stock).row
                if index == 'JKSE':
                    historical_url = portfolio_worksheet.acell('C' + str(stock_row_p)).value
                else:
                    historical_url = portfolio_worksheet.acell('E' + str(stock_row_p)).value
                std = self.connect_to_historical(historical_url, stock).get('P15:P15')
                stock_row_g = global_worksheet.find(stock).row
                self.final_data.append({'range': f'D{stock_row_g}', 'values': [[float(std[0][0].replace('%',''))]]})
                logging.info(f'{std} for {stock} found')
                if len(self.final_data) == 10:
                    global_worksheet.batch_update(self.final_data)
                    self.final_data = []
                time.sleep(5)
            except Exception as e:
                logging.info(f"Error in {stock_data} {str(e)}")
                continue
        if self.final_data:
            global_worksheet.batch_update(self.final_data)


if __name__ == '__main__':
    d = DumpStd()
    d.get_std_data_and_update()
    # portfolio_sheet = d.connect_to_gs(
    #     'https://docs.google.com/spreadsheets/d/10l-bDkh7VdCVtF5VR4r6gLBRv3PKEB6Biz9q4hBeM54/edit#gid=207780498')
    # worksheet = portfolio_sheet.worksheet('Config')
    # stock_row = worksheet.find('NFYS').row
    # print(worksheet.acell('E' + str(stock_row)).value)
    # historical_url = worksheet.acell('E' + str(stock_row)).value
    # std = d.connect_to_historical(historical_url, 'NFYS').get('P15:P15')
    # print(std)
    #
    # global_index = d.connect_to_gs(d.config['global_index'])
    # worksheet = global_index.worksheet('Supporting Data')
    # stock_row = worksheet.find('NFYS').row
    # data = [{'range': f'D{stock_row}',
    #          'values': std}]
    # worksheet.batch_update(data)
