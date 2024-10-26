import scrapy
import re
from selenium.webdriver import ActionChains
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.support.ui import Select
import time
from random import choice
from scrapy.selector import Selector
from scrapy.utils.project import get_project_settings
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
import pandas as pd
import os

class DivulgaContasSpider(scrapy.Spider):
    name = "divulga_contas"
    allowed_domains = ["divulgacandcontas.tse.jus.br"]
    start_urls = ["https://divulgacandcontas.tse.jus.br/divulga"]
    states = [ "SP", "SE", "TO"]
    def __init__(self):
        settings=get_project_settings()
 
        self.options = webdriver.ChromeOptions()
        # self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument('user-agent={0}'.format(settings.get('USER_AGENT')))

    
    def parse(self, response):
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=self.options)
        self.wait = WebDriverWait(self.driver, 3)
        self.actions = ActionChains(self.driver)
        
        for state in self.states:
            # join url with state
            if not os.path.exists(f"./dataframe/{state}"):
                os.mkdir(f"./dataframe/{state}")
            url = response.urljoin(f"/divulga/#/estados/2022/2040602022/{state}/candidatos")
            self.driver.get(url)
            self.wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[1]/div/div/section[3]/div/div/div/table/tbody/tr/td[1]/select")))
            cargos = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div/section[3]/div/div/div/table/tbody/tr/td[1]/select")
            select = Select(cargos)
            option_list = select.options
            for option in option_list:
                select.select_by_value(option.get_attribute("value"))
                time.sleep(1)
                try:
                    self.wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[1]/div/div/section[3]/div/div/table[1]/tbody/tr[1]/td[1]/a")))
                except:
                    continue
                selector = Selector(text=self.driver.page_source)
                # extract table header
                header = selector.xpath("/html/body/div[2]/div[1]/div/div/section[3]/div/div/table[1]/thead/tr/th/text()").extract()
                
                # get all candidates
                candidates_row = selector.xpath("/html/body/div[2]/div[1]/div/div/section[3]/div/div/table[1]/tbody/tr")
                rows = []
                for condidate in candidates_row:
                    candidate_attrs = condidate.xpath(".//td//text()").extract()
                    # remover empty strings
                    candidate_attrs = list(filter(lambda x: x.strip() != "", candidate_attrs))

                    # verifica se cada linha tem o mesmo tamanho do header
                    if len(candidate_attrs) != len(header):
                        # adiciona coluna faltante
                        candidate_attrs = candidate_attrs + [""]
                    # adiciona url
                    condidate_url = condidate.xpath(".//td[1]/a/@href").extract_first()
                    candidate_attrs = [condidate_url] + candidate_attrs
                    rows.append(candidate_attrs)
                      
                # create df
                header = ["url"] + header
                df = pd.DataFrame(rows, columns=header)
                # save df

                # extrair gastos eleicao
                for index, row in df.iterrows():
                    gasto_total, lista_gastos = None, None
                    try:
                        self.driver.execute_script("window.open('');")
                        self.driver.switch_to.window(self.driver.window_handles[1])
                        gasto_total, lista_gastos, doadores, fornecedores = self.extracao_dados_gastos("https://divulgacandcontas.tse.jus.br/divulga/" + row["url"])
                    except:
                        continue
                    finally:
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                    
                    df.loc[index, "gasto_total"] = gasto_total
                    # criar novo df para gastos
                    df_gastos = pd.DataFrame(lista_gastos)
                    df_doadores = pd.DataFrame(doadores)
                    df_fornecedores = pd.DataFrame(fornecedores)
                    if not os.path.exists(f"./dataframe/{state}/{option.accessible_name}_gastos_detalhe/"):
                        os.mkdir(f"./dataframe/{state}/{option.accessible_name}_gastos_detalhe/")
                    if not os.path.exists(f"./dataframe/{state}/{option.accessible_name}_doadores/"):
                        os.mkdir(f"./dataframe/{state}/{option.accessible_name}_doadores/")
                    if not os.path.exists(f"./dataframe/{state}/{option.accessible_name}_fornecedores/"):
                        os.mkdir(f"./dataframe/{state}/{option.accessible_name}_fornecedores/")
                    
                    try:
                        nome_urna = row['Nome na Urna '].replace("/", "-").replace("\"", "").replace("\t", "").replace("\n", "").replace("\r", "")
                        df_fornecedores.to_csv(f"./dataframe/{state}/{option.accessible_name}_fornecedores/{nome_urna}.csv", index=False)
                        df_doadores.to_csv(f"./dataframe/{state}/{option.accessible_name}_doadores/{nome_urna}.csv", index=False)
                        df_gastos.to_csv(f"./dataframe/{state}/{option.accessible_name}_gastos_detalhe/{nome_urna}.csv", index=False)
                    except:
                        continue
                
                df.to_csv(f"./dataframe/{state}/{option.accessible_name}.csv", index=False)
    
    def extracao_dados_gastos(self, url):
        print("##############################################################################")
        # abrir em nova aba
        
        self.driver.get(url)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[1]/div/div[2]/section[3]/div/div[1]/div[1]/h3/a")))
        
        selector = Selector(text=self.driver.page_source)
        doadores = []
        doadores_itens = selector.xpath("/html/body/div[2]/div[1]/div/div[2]/section[3]/div/div[2]//div[contains(@class, 'content')]")
        for doador in doadores_itens:
            nome = doador.xpath(".//h5/text()").extract_first()
            documento = doador.xpath("./div[contains(@class, 'text-left')]/small[1]/text()").extract_first()
            valor = doador.xpath("./div[contains(@class, 'text-left')]/small[2]/text()").extract_first()
            valor = re.findall(r'[\d,]+', valor)
            valor = float("".join(valor).replace(",", "."))

            doadores.append({
                "nome": nome,
                "documento": documento,
                "valor": valor
            })
        

        fornecedores = []
        fornecedores_itens = selector.xpath("/html/body/div[2]/div[1]/div/div[2]/section[3]/div/div[3]//div[contains(@class, 'content')]")
        for fornecedor in fornecedores_itens:
            nome = fornecedor.xpath(".//h5/text()").extract_first()
            documento = fornecedor.xpath("./div[contains(@class, 'text-left')]/small[1]/text()").extract_first()
            valor = fornecedor.xpath("./div[contains(@class, 'text-left')]/small[2]/text()").extract_first()
            valor = re.findall(r'[\d,]+', valor)
            valor = float("".join(valor).replace(",", "."))
            fornecedores.append({
                "nome": nome,
                "documento": documento,
                "valor": valor
            })

        
        element = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div[2]/section[3]/div/div[1]/div[1]/h3/a")
        element.click()

        time.sleep(1.5)
        selector = Selector(text=self.driver.page_source)
        gasto_total = selector.xpath("/html/body/div[2]/div[1]/div/div/section[3]/div/div/div[2]/div/div/span[1]/text()").extract_first()
        gasto_total = re.findall(r'[\d,]+', gasto_total)
        gasto_total = float("".join(gasto_total).replace(",", "."))
        gastos_itens = selector.xpath("/html/body/div[2]/div[1]/div/div/section[3]/div/div/div[contains(@class, 'dvg-painel-ranking')]")

        lista_gastos = []
        for item in gastos_itens:
            item_nome = item.xpath(".//h5/text()").extract_first()
            quantidade_lancamento = item.xpath("./div[contains(@class, 'text-left')]/small[1]/text()").extract_first()
            gastos_declarados = item.xpath("./div[contains(@class, 'text-left')]/small[2]/text()").extract_first()
            print(gastos_declarados)
            # extrair numero
            quantidade_lancamento = re.findall(r'\d+', quantidade_lancamento)
            quantidade_lancamento = int("".join(quantidade_lancamento))
            gastos_declarados = re.findall(r'[\d,]+', gastos_declarados)
            gastos_declarados = float("".join(gastos_declarados).replace(",", "."))

            print(item_nome, quantidade_lancamento, gastos_declarados)
            lista_gastos.append({
                "item_nome": item_nome,
                "quantidade_lancamento": quantidade_lancamento,
                "gastos_declarados": gastos_declarados
            })
        
        print(gasto_total, lista_gastos, doadores, fornecedores)
        print("##############################################################################")
        # fechar aba
        return gasto_total, lista_gastos, doadores, fornecedores