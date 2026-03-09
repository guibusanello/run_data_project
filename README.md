# 🏃 Run Data Project — Arquitetura Medallion

Projeto de aprendizado que aplica a **arquitetura Medallion** a dados reais de corrida exportados no formato JSON. O objetivo é demonstrar como estruturar um pipeline de dados em camadas, garantindo rastreabilidade, qualidade e facilidade de análise.

---

## O que é a Arquitetura Medallion?

A arquitetura Medallion organiza os dados em três camadas progressivas, onde cada camada agrega mais qualidade e valor ao dado anterior:

```
JSONs brutos → 🥉 Bronze → 🥈 Silver → 🥇 Gold
```

### 🥉 Bronze — Ingestão fiel
A primeira camada recebe os dados **exatamente como vieram da fonte**, sem nenhuma transformação. O objetivo é preservar o dado original para garantir rastreabilidade e auditoria.

- Todos os 305 arquivos JSON são consolidados em um único arquivo Parquet
- Nenhum campo é alterado, removido ou corrigido
- Apenas dois campos de controle são adicionados: `_source_file` e `_ingested_at`
- Se algo der errado nas camadas seguintes, o Bronze permite reprocessar tudo do zero

### 🥈 Silver — Limpeza e normalização
A segunda camada **transforma os dados brutos em algo consultável**. Aqui os dados são limpos, tipados e normalizados, mas ainda não são agregados.

- O array `features` (JSON aninhado) é desmontado em colunas individuais: métricas de corrida, clima, localização e segmentos mais rápidos
- Timestamps em milissegundos são convertidos para datetime UTC
- Unidades são padronizadas: duração em minutos, distância em km, pace em min/km
- Tipos corretos são aplicados: `Int64` para inteiros, `float64` para decimais, `datetime` para datas
- Resultado: 30 colunas limpas e prontas para consulta

### 🥇 Gold — Métricas analíticas
A terceira camada **responde perguntas de negócio** com dados agregados e prontos para consumo. Cada tabela Gold existe para um propósito analítico específico.

| Tabela | Descrição |
|---|---|
| `monthly_summary` | Volume mensal: km, corridas, pace médio, calorias |
| `weekly_summary` | Evolução semanal de pace e distância |
| `yearly_summary` | Visão anual consolidada com maior corrida do ano |
| `personal_bests` | Melhores tempos nos segmentos 1km, 1mi e 5km |

---

## Por que Medallion?

| Problema | Solução |
|---|---|
| Dado original corrompido ou perdido | Bronze preserva a fonte intacta |
| Bug descoberto meses depois | Reprocessa Silver e Gold a partir do Bronze |
| Mudança de regra de negócio | Atualiza apenas a camada afetada |
| Múltiplos casos de uso analíticos | Gold tem uma tabela por pergunta |
| Auditoria de origem dos dados | `_source_file` e `_ingested_at` em toda linha |

---

## Stack

- **Python 3.12** — linguagem principal
- **pandas** — transformações e manipulação de dados
- **DuckDB** — consultas SQL direto nos arquivos Parquet
- **pyarrow** — leitura e escrita de Parquet
- **Jupyter** — exploração interativa dos dados

---

## Estrutura do Projeto

```
run_data_project/
├── data/
│   ├── raw/          ← JSONs originais (nunca modificar)
│   ├── bronze/       ← runs.parquet (ingestão fiel)
│   ├── silver/       ← runs.parquet (dados normalizados)
│   └── gold/         ← monthly_summary, weekly_summary,
│                        yearly_summary, personal_bests
├── src/
│   ├── bronze.py     ← ingestão dos JSONs
│   ├── silver.py     ← limpeza e normalização
│   └── gold.py       ← agregações e métricas
├── notebooks/
│   └── explore.ipynb ← exploração com DuckDB
└── requirements.txt
```

---

## Como Rodar

### 1. Clonar e configurar o ambiente

```bash
git clone <repo>
cd run_data_project

python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Adicionar os dados

Copie seus arquivos JSON para `data/raw/`.

### 3. Executar o pipeline

```bash
# Execute em ordem
python src/bronze.py
python src/silver.py
python src/gold.py
```

### 4. Explorar os dados

```bash
jupyter notebook
```

Abra `notebooks/explore.ipynb` e use DuckDB para consultar qualquer camada:

```python
import duckdb
con = duckdb.connect()

# Exemplo: resumo anual
con.execute("SELECT * FROM read_parquet('data/gold/yearly_summary.parquet')").df()
```

---

## Princípios do Pipeline

- **Idempotência** — rodar qualquer script duas vezes produz o mesmo resultado
- **Imutabilidade do Bronze** — os JSONs originais nunca são modificados
- **Fluxo unidirecional** — os dados fluem sempre Bronze → Silver → Gold, nunca o contrário
- **Rastreabilidade** — toda linha sabe de qual arquivo veio e quando foi ingerida
