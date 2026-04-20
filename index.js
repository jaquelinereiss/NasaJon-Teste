import fs from "fs";
import csv from "csv-parser";
import axios from "axios";
import { createObjectCsvWriter } from "csv-writer";
import stringSimilarity from "string-similarity";
import dotenv from "dotenv";

dotenv.config();

const IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios";

function normalize(str) {
  return str
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase();
}

async function fetchMunicipios() {
  try {
    const res = await axios.get(IBGE_URL);
    return res.data;
  } catch (err) {
    console.error("Erro ao buscar IBGE");
    return null;
  }
}

function findBestMatch(input, municipios) {
  const normalizedInput = normalize(input);

  let bestMatch = null;
  let bestScore = 0;

  municipios.forEach((m) => {
    const nome = normalize(m.nome);
    const score = stringSimilarity.compareTwoStrings(normalizedInput, nome);

    if (score > bestScore) {
      bestScore = score;
      bestMatch = m;
    }
  });

  if (bestScore > 0.7) return bestMatch;
  return null;
}

async function processar() {
  const municipiosIBGE = await fetchMunicipios();

  const resultados = [];

  const stats = {
    total_municipios: 0,
    total_ok: 0,
    total_nao_encontrado: 0,
    total_erro_api: 0,
    pop_total_ok: 0,
    por_regiao: {}
  };

  return new Promise((resolve) => {
    fs.createReadStream("input.csv")
      .pipe(csv())
      .on("data", (row) => {
        stats.total_municipios++;

        if (!municipiosIBGE) {
          stats.total_erro_api++;

          resultados.push({
            municipio_input: row.municipio,
            populacao_input: row.populacao,
            municipio_ibge: "",
            uf: "",
            regiao: "",
            id_ibge: "",
            status: "ERRO_API"
          });

          return;
        }

        const match = findBestMatch(row.municipio, municipiosIBGE);

        if (!match) {
          stats.total_nao_encontrado++;

          resultados.push({
            municipio_input: row.municipio,
            populacao_input: row.populacao,
            municipio_ibge: "",
            uf: "",
            regiao: "",
            id_ibge: "",
            status: "NAO_ENCONTRADO"
          });

          return;
        }

        const regiao = match.microrregiao.mesorregiao.UF.regiao.nome;

        stats.total_ok++;
        stats.pop_total_ok += Number(row.populacao);

        if (!stats.por_regiao[regiao]) {
          stats.por_regiao[regiao] = {
            total: 0,
            count: 0
          };
        }

        stats.por_regiao[regiao].total += Number(row.populacao);
        stats.por_regiao[regiao].count++;

        resultados.push({
          municipio_input: row.municipio,
          populacao_input: row.populacao,
          municipio_ibge: match.nome,
          uf: match.microrregiao.mesorregiao.UF.sigla,
          regiao: regiao,
          id_ibge: match.id,
          status: "OK"
        });
      })
      .on("end", async () => {
        const medias_por_regiao = {};

        for (const regiao in stats.por_regiao) {
          medias_por_regiao[regiao] =
            stats.por_regiao[regiao].total /
            stats.por_regiao[regiao].count;
        }

        const finalStats = {
          total_municipios: stats.total_municipios,
          total_ok: stats.total_ok,
          total_nao_encontrado: stats.total_nao_encontrado,
          total_erro_api: stats.total_erro_api,
          pop_total_ok: stats.pop_total_ok,
          medias_por_regiao
        };

        await gerarCSV(resultados);

        await enviarStats(finalStats);

        resolve();
      });
  });
}

async function gerarCSV(data) {
  const writer = createObjectCsvWriter({
    path: "resultado.csv",
    header: [
      { id: "municipio_input", title: "municipio_input" },
      { id: "populacao_input", title: "populacao_input" },
      { id: "municipio_ibge", title: "municipio_ibge" },
      { id: "uf", title: "uf" },
      { id: "regiao", title: "regiao" },
      { id: "id_ibge", title: "id_ibge" },
      { id: "status", title: "status" }
    ]
  });

  await writer.writeRecords(data);
}

async function enviarStats(stats) {
  const url = process.env.PROJECT_FUN_URL;
  const token = process.env.ACCESS_TOKEN;

  try {
    const res = await axios.post(
      url,
      { stats },
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json"
        }
      }
    );

    console.log("Score:", res.data.score);
    console.log("Feedback:", res.data.feedback);
  } catch (err) {
    console.error("Erro ao enviar stats");
  }
}

processar();