import scrapy
from scrapy.selector import Selector
from bs4 import BeautifulSoup
from functools import reduce

class QuotesSpider(scrapy.Spider):
    name = "deputados"

    def start_requests(self):
        urls_deputadas = []
        urls_deputados = []

        with open('lista_deputadas_string_list_txt.txt') as f:
            lista_deputadas = f.readlines()

            for link in lista_deputadas:
                urls_deputadas.append(link.split('"')[1])


        for url in urls_deputadas:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(genero='feminino'))

        with open('lista_deputados_string_list.txt') as f:
            lista_deputados = f.readlines()

            for link in lista_deputados:
                urls_deputados.append(link.split('"')[1])

        for url in urls_deputados:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(genero='masculino'))


    def parse(self, response, genero):
        dep_info = self.dep_info(response, genero)
        presencas = self.dep_presenca(response)
        gastos_par = self.gastos_parlamentar(response)
        gastos_gab = self.gastos_parlamentar(response)

        yield {
            **dep_info,
            **gastos_par,
            **gastos_gab,
            **presencas,
        }

    def dep_info(self, response, genero):
        dep_info = response.css("ul.informacoes-deputado").getall()
        data = BeautifulSoup(response.body, "html")
        salario_bruto = data.find_all("section", {"id": "recursos-section"})[0].find_all("a", {"class": "beneficio__info"})[1]
        salario_bruto = float(salario_bruto.get_text().split("R$")[1].strip().replace(".", '').replace(",", ".").strip())
        
        for i in dep_info:
            body = i
            nome = Selector(text=body).xpath('//ul/li[1]').get()
            data = Selector(text=body).xpath('//ul/li[5]').get()

            if data == None:
                data = Selector(text=body).xpath('//ul/li[2]').get()
            
            return {"nome": nome[29:-5], "data_nascimento": data[37:-5], "genero": genero, "salario_bruto": salario_bruto}

    def dep_presenca(self, response):
        data = BeautifulSoup(response.body, "html")
        presencas = data.find_all("dd", {"class": "list-table__definition-description"})

        if (presencas):
            return {
                "presenca_plenario": int(presencas[0].get_text().strip().split(" ")[0].strip()),
                "ausencia_plenario": int(presencas[1].get_text().strip().split(" ")[0].strip()) + int(presencas[2].get_text().strip().split(" ")[0].strip()),
                "ausencia_justificada_plenario":  int(presencas[2].get_text().strip().split(" ")[0].strip()),
                "presenca_comissao": int(presencas[3].get_text().strip().split(" ")[0].strip()),
                "ausencia_comissao": int(presencas[4].get_text().strip().split(" ")[0].strip()) + int(presencas[5].get_text().strip().split(" ")[0].strip()),
                "ausencia_justificada_comissao": int(presencas[5].get_text().strip().split(" ")[0].strip())
            }

        return {
            "presenca_plenario": None,
            "ausencia_plenario": None,
            "ausencia_justificada_plenario":  None,
            "presenca_comissao": None,
            "ausencia_comissao": None,
            "ausencia_justificada_comissao": None
        }

    def gastos_parlamentar(self, response):
        data = BeautifulSoup(response.body, "html")
        gastos_parlamentar = data.find_all("table", {"id": "gastomensalcotaparlamentar"})[0].find_all("td")
  
        gastos_formatted = self.format_values(gastos_parlamentar)
        soma_gastos = self.get_total_gastos(gastos_formatted)
        
        return {
            "gasto_jan_gab": gastos_formatted[0]["gasto"] if gastos_formatted[0:] else None,
            "gasto_fev_gab": gastos_formatted[1]["gasto"] if gastos_formatted[1:] else None,
            "gasto_mar_gab": gastos_formatted[2]["gasto"] if gastos_formatted[2:] else None,
            "gasto_abr_gab": gastos_formatted[3]["gasto"] if gastos_formatted[3:] else None,
            "gasto_maio_gab": gastos_formatted[4]["gasto"] if gastos_formatted[4:] else None,
            "gasto_junho_gab": gastos_formatted[5]["gasto"] if gastos_formatted[5:] else None,
            "gasto_jul_gab": gastos_formatted[6]["gasto"] if gastos_formatted[6:] else None,
            "gasto_agosto_gab": gastos_formatted[7]["gasto"] if gastos_formatted[7:] else None,
            "gasto_set_gab": gastos_formatted[8]["gasto"] if gastos_formatted[8:] else None,
            "gasto_out_gab": gastos_formatted[9]["gasto"] if gastos_formatted[9:] else None,
            "gasto_nov_gab": gastos_formatted[10]["gasto"] if gastos_formatted[10:] else None,
            "gasto_dez_gab": gastos_formatted[11]["gasto"] if gastos_formatted[11:] else None,
            "gasto_total_gab": soma_gastos,
        }

    def gastos_gabinete(self, response):
        data = BeautifulSoup(response.body, "html")
        gastos_gabinete = data.find_all("table", {"id": "gastomensalverbagabinete"})[0].find_all("td")
  
        gastos_formatted = self.format_values(gastos_gabinete)
        soma_gastos = self.get_total_gastos(gastos_formatted)

        return {
            "gasto_jan_par": gastos_formatted[0]["gasto"] if gastos_formatted[0:] else None,
            "gasto_fev_par": gastos_formatted[1]["gasto"] if gastos_formatted[1:] else None,
            "gasto_mar_par": gastos_formatted[2]["gasto"] if gastos_formatted[2:] else None,
            "gasto_abr_par": gastos_formatted[3]["gasto"] if gastos_formatted[3:] else None,
            "gasto_maio_par": gastos_formatted[4]["gasto"] if gastos_formatted[4:] else None,
            "gasto_junho_par": gastos_formatted[5]["gasto"] if gastos_formatted[5:] else None,
            "gasto_jul_par": gastos_formatted[6]["gasto"] if gastos_formatted[6:] else None,
            "gasto_agosto_par": gastos_formatted[7]["gasto"] if gastos_formatted[7:] else None,
            "gasto_set_par": gastos_formatted[8]["gasto"] if gastos_formatted[8:] else None,
            "gasto_out_par": gastos_formatted[9]["gasto"] if gastos_formatted[9:] else None,
            "gasto_nov_par": gastos_formatted[10]["gasto"] if gastos_formatted[10:] else None,
            "gasto_dez_par": gastos_formatted[11]["gasto"] if gastos_formatted[11:] else None,
            "gasto_total_par": soma_gastos,
        }

    def format_values(self, tabela):
        gastos = []

        for value in range(0, len(tabela), 3):
            gastos.append({
                "mes": tabela[value].get_text(),
                "gasto": float(tabela[value + 1].get_text().replace(".", '').replace(",", ".").strip())
            })
        
        return gastos

    def get_total_gastos(self, gastos):
        gastos = [gasto["gasto"] for gasto in gastos]
        def soma(a, b):
            x = a if a else 0
            y = b if b else 0

            return x + y 
        return reduce(soma, gastos)


            

        