"""
pipeline_importacao.py
======================
Pipeline ETL — Controle de Importações → Modelo Estrela (Power BI)
Grão: 1 linha por Processo + Embarque

Execução:
    python pipeline_importacao.py

Saídas:
    output/fato_importacao.csv
    output/dim_fornecedor.csv
    output/dim_modal.csv
    output/dim_data.csv
"""

import os
import re
import pandas as pd
import numpy as np
from datetime import datetime

# ──────────────────────────────────────────────
# CONFIGURAÇÕES
# ──────────────────────────────────────────────
FILE_PATH  = "Controle de Importações.xlsx"
SHEET_NAME = "Controle PIs"
OUTPUT_DIR = "output"

# Limites de sanidade para valores monetários (USD)
MAX_VALOR_MONETARIO = 50_000_000   # 50 milhões USD — ajuste se necessário
MAX_LEAD_TIME_DIAS  = 365          # 1 ano
MIN_LEAD_TIME_DIAS  = 0            # lead time negativo = dado inválido

# Mapeamento canônico: nome_padronizado → aliases possíveis na planilha
COLUMN_MAP = {
    "processo"                  : ["processo"],
    "embarque"                  : ["embarque"],
    "no_da_pi"                  : ["no da pi", "n° da pi", "nº da pi", "no_da_pi"],
    "supplier"                  : ["supplier", "fornecedor"],
    "trader"                    : ["trader"],
    "status"                    : ["status"],
    "quantidade"                : ["quantidade", "qtd"],
    "valor_total_pi"            : ["valor total pi", "valor_total_pi"],
    "frete"                     : ["$ frete", "frete", "$_frete"],
    "valor_total_a_pagar_carga" : ["valor total a pagar (carga)", "valor_total_a_pagar_carga"],
    "previsao_impostos"         : ["previsão impostos", "previsao impostos", "previsao_impostos"],
    "deposito_numerario"        : ["depósito numerário", "deposito numerario", "deposito_numerario"],
    "numerario_considerado"     : ["numerário considerado", "numerario considerado", "numerario_considerado"],
    "numerario_considerado_ipi" : ["numerário considerado c/ ipi", "numerario considerado c/ ipi", "numerario_considerado_ipi"],
    "ptax_pi"                   : ["ptax pi", "ptax_pi"],
    "ptax_di"                   : ["ptax di", "ptax_di"],
    "modal"                     : ["modal"],
    "etd_china"                 : ["etd china", "etd_china"],
    "eta_santos"                : ["eta santos", "eta_santos"],
    "etd_booking"               : ["etd booking", "etd_booking"],
    "eta_booking"               : ["eta booking", "eta_booking"],
    "dias_desembaraco"          : ["dias desembaraço", "dias desembaraco", "dias_desembaraco"],
    "entrega_gocase"            : ["entrega gocase", "entrega_gocase"],
    "pallets"                   : ["pallets"],
    "registro_di"               : ["registro di", "registro_di"],
    "liberacao_sefaz"           : ["liberação sefaz", "liberacao sefaz", "liberacao_sefaz"],
    "nf"                        : ["nf"],
    "faturamento_final"         : ["faturamento final", "faturamento_final"],
}

DATE_COLS = [
    "etd_china", "eta_santos", "etd_booking",
    "eta_booking", "entrega_gocase", "registro_di",
    "liberacao_sefaz", "faturamento_final",
]

# Colunas que devem ser numéricas — tratadas com cuidado especial
NUMERIC_COLS = [
    "quantidade", "valor_total_pi", "frete",
    "valor_total_a_pagar_carga", "previsao_impostos",
    "deposito_numerario", "numerario_considerado",
    "numerario_considerado_ipi", "ptax_pi", "ptax_di",
    "dias_desembaraco", "pallets",
]

# Colunas de valor monetário — sujeitas ao filtro de sanidade
MONETARY_COLS = [
    "valor_total_pi", "frete", "numerario_considerado",
    "previsao_impostos", "deposito_numerario", "numerario_considerado_ipi",
    "valor_total_a_pagar_carga",
]

MONTH_PT = {
    1: "Janeiro", 2: "Fevereiro",  3: "Março",     4: "Abril",
    5: "Maio",    6: "Junho",      7: "Julho",      8: "Agosto",
    9: "Setembro",10: "Outubro",  11: "Novembro",  12: "Dezembro",
}


# ──────────────────────────────────────────────
# UTILITÁRIOS
# ──────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def slugify(text: str) -> str:
    """snake_case sem acentos."""
    text = str(text).strip().lower()
    for src, dst in [("á","a"),("à","a"),("ã","a"),("â","a"),("ä","a"),
                     ("é","e"),("è","e"),("ê","e"),("ë","e"),
                     ("í","i"),("ì","i"),("î","i"),("ï","i"),
                     ("ó","o"),("ò","o"),("õ","o"),("ô","o"),("ö","o"),
                     ("ú","u"),("ù","u"),("û","u"),("ü","u"),
                     ("ç","c"),("ñ","n")]:
        text = text.replace(src, dst)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    log(f"Diretório de saída: '{path}'")


def safe_divide(num: pd.Series, den: pd.Series) -> pd.Series:
    """Divisão segura — NaN onde denominador é zero/nulo."""
    den_safe = den.replace(0, np.nan)
    return num / den_safe


def parse_br_number(series: pd.Series) -> pd.Series:
    """
    Converte números em formato brasileiro para float.
    Suporta: '1.234.567,89' | '1234567.89' | '1,23E+10' | '9.57E+15'
    """
    s = series.astype(str).str.strip()

    # Mascara notação científica antes de tratar separadores
    is_sci = s.str.upper().str.contains(r"E[+\-]\d", regex=True, na=False)

    # Para não-científicos: remove pontos de milhar e converte vírgula decimal
    s_normal = s.copy()
    # Detecta se é formato BR (tem vírgula como decimal): ex '1.234,56'
    has_comma_decimal = s_normal.str.contains(r"\d,\d", regex=True, na=False)

    s_normal = s_normal.where(
        ~has_comma_decimal,
        s_normal.str.replace(r"\.", "", regex=True).str.replace(",", ".", regex=False)
    )
    # Para formato americano já com ponto: remove vírgulas de milhar
    s_normal = s_normal.str.replace(",", "", regex=False)

    result = pd.to_numeric(s_normal, errors="coerce")
    return result


# ──────────────────────────────────────────────
# ETAPA 1 — LEITURA
# ──────────────────────────────────────────────
def read_source(file_path: str, sheet_name: str) -> pd.DataFrame:
    log(f"Lendo '{file_path}' | aba '{sheet_name}'...")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        log(f"  → {len(df)} linhas | {len(df.columns)} colunas originais")
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    except Exception as e:
        raise RuntimeError(f"Erro ao ler planilha: {e}")


# ──────────────────────────────────────────────
# ETAPA 2 — PADRONIZAÇÃO DE COLUNAS
# ──────────────────────────────────────────────
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    log("Padronizando nomes de colunas...")
    df.columns = [slugify(c) for c in df.columns]

    reverse_map = {}
    for canonical, aliases in COLUMN_MAP.items():
        for alias in aliases:
            reverse_map[slugify(alias)] = canonical

    renamed = {col: reverse_map[col] for col in df.columns if col in reverse_map}
    df = df.rename(columns=renamed)

    found     = [c for c in COLUMN_MAP if c in df.columns]
    not_found = [c for c in COLUMN_MAP if c not in df.columns]
    log(f"  → {len(found)} colunas mapeadas | {len(not_found)} ausentes: {not_found or 'nenhuma'}")
    return df


# ──────────────────────────────────────────────
# ETAPA 3 — LIMPEZA DE STRINGS
# ──────────────────────────────────────────────
def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços extras e caracteres invisíveis de colunas de texto."""
    log("Limpando strings...")
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)   # espaços múltiplos
            .str.replace(r"[\x00-\x1f]", "", regex=True)  # chars de controle
            .replace({"nan": np.nan, "None": np.nan, "": np.nan, "N/A": np.nan,
                      "-": np.nan, "—": np.nan, "#N/A": np.nan})
        )
    return df


# ──────────────────────────────────────────────
# ETAPA 4 — CONVERSÃO NUMÉRICA
# ──────────────────────────────────────────────
def cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    log("Convertendo colunas numéricas...")
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = parse_br_number(df[col])
    return df


# ──────────────────────────────────────────────
# ETAPA 5 — CONVERSÃO DE DATAS
# ──────────────────────────────────────────────
def cast_dates(df: pd.DataFrame) -> pd.DataFrame:
    log("Convertendo colunas de data...")
    for col in DATE_COLS:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.strip()

        # Tenta ISO primeiro (vindo do Excel como datetime)
        parsed = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")

        # Fallback: formato BR dd/mm/yyyy
        mask = parsed.isna() & s.notna() & (s != "nan")
        if mask.any():
            parsed[mask] = pd.to_datetime(s[mask], dayfirst=True, errors="coerce")

        n_invalid = parsed.isna().sum()
        if n_invalid > 0:
            log(f"  ⚠ {col}: {n_invalid} data(s) inválida(s) → NaT")
        df[col] = parsed
    return df


# ──────────────────────────────────────────────
# ETAPA 6 — SANIDADE DE VALORES MONETÁRIOS
# ──────────────────────────────────────────────
def validate_monetary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nullifica valores monetários fora do intervalo esperado.
    Valores > MAX_VALOR_MONETARIO são provavelmente dados corrompidos
    (ex: célula com fórmula quebrada, formato numérico errado no Excel).
    """
    log(f"Validando valores monetários (limite: USD {MAX_VALOR_MONETARIO:,})...")
    total_invalidos = 0
    for col in MONETARY_COLS:
        if col not in df.columns:
            continue
        mask_invalido = (df[col] > MAX_VALOR_MONETARIO) | (df[col] < 0)
        n = mask_invalido.sum()
        if n > 0:
            log(f"  ⚠ {col}: {n} valor(es) fora do range → NaN")
            df.loc[mask_invalido, col] = np.nan
            total_invalidos += n

    # PTAX: valores esperados entre 1 e 20
    for col in ["ptax_pi", "ptax_di"]:
        if col in df.columns:
            mask = (df[col] > 20) | (df[col] < 1)
            n = mask.sum()
            if n > 0:
                log(f"  ⚠ {col}: {n} PTAX fora de [1–20] → NaN")
                df.loc[mask, col] = np.nan

    # Quantidade e pallets: não podem ser negativos
    for col in ["quantidade", "pallets"]:
        if col in df.columns:
            mask = df[col] < 0
            n = mask.sum()
            if n > 0:
                log(f"  ⚠ {col}: {n} valor(es) negativo(s) → NaN")
                df.loc[mask, col] = np.nan

    log(f"  → Total de células nullificadas: {total_invalidos}")
    return df


# ──────────────────────────────────────────────
# ETAPA 7 — TRATAMENTO DE NULOS
# ──────────────────────────────────────────────
def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    log("Tratando valores nulos...")

    # Monetárias: NaN mantido (não forçar 0 — zero tem significado diferente de ausente)
    # Apenas frete e quantidade usam 0 como padrão válido quando ausente
    for col in ["frete"]:
        if col in df.columns:
            n = df[col].isna().sum()
            if n > 0:
                log(f"  → {col}: {n} nulos → 0")
            df[col] = df[col].fillna(0)

    # Textuais
    for col in ["supplier", "trader", "status", "modal"]:
        if col in df.columns:
            df[col] = df[col].fillna("Não Informado").str.strip().str.title()

    # Pallets: 0 quando ausente (sem palete informado)
    if "pallets" in df.columns:
        df["pallets"] = df["pallets"].fillna(0)

    return df


# ──────────────────────────────────────────────
# ETAPA 8 — MÉTRICAS
# ──────────────────────────────────────────────
def convert_numerario_to_usd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte numerario_considerado de BRL para USD usando PTAX DI.
    Fallback para PTAX PI quando PTAX DI não está disponível.
    Garante que custo_total_real fique inteiramente em USD.
    """
    log("Convertendo numerário de BRL para USD...")

    if "numerario_considerado" not in df.columns:
        log("  ⚠ numerario_considerado ausente — pulando conversão")
        return df

    ptax_efetivo = df.get("ptax_di", pd.Series(np.nan, index=df.index))
    if "ptax_pi" in df.columns:
        ptax_efetivo = ptax_efetivo.fillna(df["ptax_pi"])

    n_sem_ptax = ptax_efetivo.isna().sum()
    if n_sem_ptax > 0:
        log(f"  ⚠ {n_sem_ptax} linha(s) sem PTAX → impostos_usd ficará NaN")

    df["numerario_usd"] = safe_divide(df["numerario_considerado"], ptax_efetivo).round(2)

    validos = df["numerario_usd"].dropna()
    if not validos.empty:
        log(f"  → numerario_usd | mediana: {validos.median():,.2f} | max: {validos.max():,.2f}")

    return df
def create_metrics(df: pd.DataFrame) -> pd.DataFrame:
    log("Calculando métricas derivadas...")

    vpi  = df.get("valor_total_pi",       pd.Series(np.nan, index=df.index))
    frt  = df.get("frete",                pd.Series(0.0,    index=df.index))
    num  = df.get("numerario_considerado",pd.Series(np.nan, index=df.index))
    qtd  = df.get("quantidade",           pd.Series(np.nan, index=df.index))
    pal  = df.get("pallets",              pd.Series(np.nan, index=df.index))

    # Custo total: soma apenas os componentes disponíveis
    # fillna(0) aqui apenas para a soma, não altera as colunas originais
    df["custo_total_real"] = vpi.fillna(0) + frt.fillna(0) + num.fillna(0)
    # Se todos os três são NaN, custo_total fica 0 — nullificamos
    all_null_mask = vpi.isna() & frt.isna() & num.isna()
    df.loc[all_null_mask, "custo_total_real"] = np.nan

    df["custo_unitario"]      = safe_divide(df["custo_total_real"], qtd)
    df["custo_por_pallet"]    = safe_divide(df["custo_total_real"], pal.replace(0, np.nan))
    df["variacao_cambial"]    = df.get("ptax_di", np.nan) - df.get("ptax_pi", np.nan)
    df["percentual_frete"]    = safe_divide(frt, df["custo_total_real"])
    df["percentual_impostos"] = safe_divide(num, df["custo_total_real"])

    # Lead time
    if "entrega_gocase" in df.columns and "etd_china" in df.columns:
        lt = (df["entrega_gocase"] - df["etd_china"]).dt.days
        # Nullifica lead times fora do intervalo esperado
        lt_invalido = (lt < MIN_LEAD_TIME_DIAS) | (lt > MAX_LEAD_TIME_DIAS)
        n_inv = lt_invalido.sum()
        if n_inv > 0:
            log(f"  ⚠ lead_time_total: {n_inv} valor(es) fora de [{MIN_LEAD_TIME_DIAS}–{MAX_LEAD_TIME_DIAS}] → NaN")
        lt[lt_invalido] = np.nan
        df["lead_time_total"] = lt
    else:
        df["lead_time_total"] = np.nan

    # Validação final de métricas derivadas
    for col in ["custo_unitario", "custo_por_pallet"]:
        if col in df.columns:
            mask = df[col] > MAX_VALOR_MONETARIO
            n = mask.sum()
            if n > 0:
                log(f"  ⚠ {col}: {n} valor(es) absurdo(s) pós-cálculo → NaN")
                df.loc[mask, col] = np.nan

    # Log de resumo
    ct = df["custo_total_real"].dropna()
    lt = df["lead_time_total"].dropna()
    log(f"  → custo_total_real  | mediana: {ct.median():>15,.2f} | max: {ct.max():>15,.2f} | nulos: {df['custo_total_real'].isna().sum()}")
    log(f"  → lead_time_total   | média: {lt.mean():.1f} dias | nulos: {df['lead_time_total'].isna().sum()}")

    return df


# ──────────────────────────────────────────────
# ETAPA 9 — FATO
# ──────────────────────────────────────────────
def build_fato(df: pd.DataFrame) -> pd.DataFrame:
    log("Construindo fato_importacao...")

    col_map = {
        "processo"               : "processo",
        "embarque"               : "embarque",
        "supplier"               : "supplier",
        "modal"                  : "modal",
        "etd_china"              : "data_etd",
        "entrega_gocase"         : "data_entrega",
        "quantidade"             : "quantidade",
        "valor_total_pi"         : "valor_produto",
        "frete"                  : "frete",
        "numerario_considerado"  : "impostos",
        "custo_total_real"       : "custo_total_real",
        "custo_unitario"         : "custo_unitario",
        "variacao_cambial"       : "variacao_cambial",
        "lead_time_total"        : "lead_time_total",
        "dias_desembaraco"       : "dias_desembaraco",
        "pallets"                : "pallets",
        "custo_por_pallet"       : "custo_por_pallet",
        "percentual_frete"       : "percentual_frete",
        "percentual_impostos"    : "percentual_impostos",
    }

    cols_presentes = {k: v for k, v in col_map.items() if k in df.columns}
    fato = df[list(cols_presentes.keys())].copy().rename(columns=cols_presentes)

    # Arredondamentos para leitura no Power BI
    round2 = ["valor_produto","frete","impostos","custo_total_real",
              "custo_unitario","custo_por_pallet","variacao_cambial"]
    round4 = ["percentual_frete","percentual_impostos"]
    for col in round2:
        if col in fato.columns:
            fato[col] = fato[col].round(2)
    for col in round4:
        if col in fato.columns:
            fato[col] = fato[col].round(4)

    log(f"  → {len(fato)} linhas | {len(fato.columns)} colunas")
    return fato


# ──────────────────────────────────────────────
# ETAPA 10 — DIMENSÕES
# ──────────────────────────────────────────────
def build_dim_fornecedor(df: pd.DataFrame) -> pd.DataFrame:
    log("Construindo dim_fornecedor...")
    agg = df.groupby("supplier", as_index=False).agg(
        total_importado          = ("custo_total_real", "sum"),
        custo_medio              = ("custo_total_real", "mean"),
        lead_time_medio          = ("lead_time_total",  "mean"),
        percentual_frete_medio   = ("percentual_frete",    "mean"),
        percentual_imposto_medio = ("percentual_impostos", "mean"),
    ).round(2)
    log(f"  → {len(agg)} fornecedores")
    return agg


def build_dim_modal(df: pd.DataFrame) -> pd.DataFrame:
    log("Construindo dim_modal...")

    # Normaliza variações de nome (ex: "Aereo e maritimo" → "Aéreo e Marítimo")
    modal_map = {
        "aereo"              : "Aéreo",
        "aereo e maritimo"   : "Aéreo e Marítimo",
        "fcl"                : "FCL",
        "lcl"                : "LCL",
        "nao informado"      : "Não Informado",
    }
    df["modal_norm"] = (
        df["modal"]
        .str.lower()
        .str.strip()
        .apply(lambda x: next((v for k, v in modal_map.items() if k in str(x)), x.title()))
    )

    agg = df.groupby("modal_norm", as_index=False).agg(
        total_embarques = ("embarque",         "count"),
        custo_medio     = ("custo_total_real",  "mean"),
        lead_time_medio = ("lead_time_total",   "mean"),
    ).rename(columns={"modal_norm": "modal"}).round(2)
    log(f"  → {len(agg)} modais")
    return agg


def build_dim_data(df: pd.DataFrame) -> pd.DataFrame:
    log("Construindo dim_data (calendário)...")

    all_dates = pd.Series(dtype="datetime64[ns]")
    for col in DATE_COLS:
        if col in df.columns:
            all_dates = pd.concat([all_dates, df[col].dropna()])

    if all_dates.empty:
        log("  ⚠ Nenhuma data válida → dim_data vazia")
        return pd.DataFrame(columns=["data","ano","mes","nome_mes","trimestre"])

    date_range = pd.date_range(
        start=all_dates.min().replace(day=1),
        end=all_dates.max(),
        freq="D",
    )
    dim = pd.DataFrame({"data": date_range})
    dim["ano"]       = dim["data"].dt.year
    dim["mes"]       = dim["data"].dt.month
    dim["nome_mes"]  = dim["mes"].map(MONTH_PT)
    dim["trimestre"] = dim["data"].dt.quarter
    dim["data"]      = dim["data"].dt.strftime("%d/%m/%Y")

    log(f"  → {len(dim)} dias")
    return dim


# ──────────────────────────────────────────────
# ETAPA 11 — EXPORTAÇÃO
# ──────────────────────────────────────────────
def export_csv(df: pd.DataFrame, filename: str, output_dir: str) -> None:
    path = os.path.join(output_dir, filename)
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig", sep=";", decimal=",",
                  date_format="%d/%m/%Y")
        log(f"  ✓ {path}  ({len(df)} linhas)")
    except PermissionError:
        raise PermissionError(
            f"Arquivo '{path}' está aberto em outro programa. Feche-o e tente novamente."
        )


# ──────────────────────────────────────────────
# RELATÓRIO DE QUALIDADE
# ──────────────────────────────────────────────
def quality_report(fato: pd.DataFrame) -> None:
    log("=" * 60)
    log("RELATÓRIO DE QUALIDADE — fato_importacao")
    log("=" * 60)
    log(f"  Total de registros : {len(fato)}")
    for col in fato.columns:
        n_null = fato[col].isna().sum()
        pct    = n_null / len(fato) * 100
        flag   = " ⚠" if pct > 20 else ""
        log(f"  {col:<25} nulos: {n_null:>4} ({pct:>5.1f}%){flag}")
    log("=" * 60)


# ──────────────────────────────────────────────
# ORQUESTRADOR
# ──────────────────────────────────────────────
def run_pipeline(
    file_path:  str = FILE_PATH,
    sheet_name: str = SHEET_NAME,
    output_dir: str = OUTPUT_DIR,
) -> None:
    log("=" * 60)
    log("INÍCIO DO PIPELINE — Controle de Importações")
    log("=" * 60)

    try:
        ensure_output_dir(output_dir)

        df = read_source(file_path, sheet_name)
        df = standardize_columns(df)
        df = clean_strings(df)
        df = cast_numerics(df)
        df = cast_dates(df)
        df = validate_monetary(df)
        df = handle_nulls(df)
        df = create_metrics(df)

        fato           = build_fato(df)
        dim_fornecedor = build_dim_fornecedor(df)
        dim_modal      = build_dim_modal(df)
        dim_data       = build_dim_data(df)

        quality_report(fato)

        log("Exportando CSVs...")
        export_csv(fato,           "fato_importacao.csv",  output_dir)
        export_csv(dim_fornecedor, "dim_fornecedor.csv",   output_dir)
        export_csv(dim_modal,      "dim_modal.csv",        output_dir)
        export_csv(dim_data,       "dim_data.csv",         output_dir)

        log("=" * 60)
        log("PIPELINE CONCLUÍDO COM SUCESSO")
        log("=" * 60)

    except Exception as e:
        log(f"ERRO CRÍTICO: {e}")
        raise


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()