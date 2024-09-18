require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const csv = require("csv-parser");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

let requestId = 1;

// Read events from CSV file
const events = [];

// Object to store the last processed block for each network-contract combination
const lastProcessedBlocks = {};

const logFilePath = path.join(__dirname, "process.log");

function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp}: ${message}\n`;
  console.log(message);
  fs.appendFileSync(logFilePath, logMessage);
}

async function readCSVFile(filePath, dataArray) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => dataArray.push(row))
      .on("end", resolve)
      .on("error", reject);
  });
}

async function initializeData() {
  // Delete the log file if it exists
  if (fs.existsSync(logFilePath)) {
    fs.unlinkSync(logFilePath);
    log("Previous log file deleted.");
  }

  await readCSVFile("events.csv", events);
  log(`Loaded ${events.length} events`);
}

async function getLatestBlockNumber(RPC_URL, strategy = "eth_blockNumber") {
  try {
    if (strategy === "eth_blockNumber") {
      const response = await axios.post(RPC_URL, {
        jsonrpc: "2.0",
        method: "eth_blockNumber",
        params: [],
        id: requestId++,
      });
      return parseInt(response.data.result, 16);
    } else if (strategy === "eth_getBlockByNumber") {
      const response = await axios.post(RPC_URL, {
        jsonrpc: "2.0",
        method: "eth_getBlockByNumber",
        params: ["finalized", false],
        id: requestId++,
      });
      if (response.data.result && response.data.result.number) {
        return parseInt(response.data.result.number, 16);
      } else {
        console.error("Unexpected response structure:", response.data);
        throw new Error("Invalid response structure for eth_getBlockByNumber");
      }
    } else {
      throw new Error(`Unsupported strategy: ${strategy}`);
    }
  } catch (error) {
    console.error(
      `Error in getLatestBlockNumber (${strategy}):`,
      error.message
    );
    throw error;
  }
}

async function getLogs(
  RPC_URL,
  contractAddress,
  eventTopic,
  fromBlock,
  toBlock
) {
  const response = await axios.post(RPC_URL, {
    jsonrpc: "2.0",
    method: "eth_getLogs",
    params: [
      {
        fromBlock: `0x${fromBlock.toString(16)}`,
        toBlock: `0x${toBlock.toString(16)}`,
        address: [contractAddress],
        topics: [eventTopic],
      },
    ],
    id: requestId++,
  });
  return response.data.result;
}

function getRpcUrl(chain) {
  switch (chain) {
    case "celo":
      return `https://celo-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
    case "coinbase_base":
      return `https://base-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
    case "ethereum":
      return `https://eth-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
    case "fantom":
      return `https://fantom-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
    case "linea":
      return `https://linea-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
    case "mantle":
      return `https://mantle-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`;
    default:
      throw new Error(`Unsupported chain: ${chain}`);
  }
}

async function processLogs() {
  try {
    // Group events by network
    const eventsByNetwork = events.reduce((acc, event) => {
      if (!acc[event.chain]) {
        acc[event.chain] = [];
      }
      acc[event.chain].push(event);
      return acc;
    }, {});

    for (const [network, networkEvents] of Object.entries(eventsByNetwork)) {
      const RPC_URL = getRpcUrl(network);

      // Get latest block using both strategies
      const latestBlockEthBlockNumber = await getLatestBlockNumber(
        RPC_URL,
        "eth_blockNumber"
      );
      const latestBlockEthGetBlockByNumber = await getLatestBlockNumber(
        RPC_URL,
        "eth_getBlockByNumber"
      );

      // Process logs for both strategies
      await processNetworkLogs(
        networkEvents,
        RPC_URL,
        latestBlockEthBlockNumber,
        "eth_blockNumber"
      );
      await processNetworkLogs(
        networkEvents,
        RPC_URL,
        latestBlockEthGetBlockByNumber,
        "eth_getBlockByNumber"
      );
    }
  } catch (error) {
    console.error("Error processing logs:", error.message);
    log("Error processing logs: " + error.message);
  }
}

async function processNetworkLogs(
  networkEvents,
  RPC_URL,
  latestBlock,
  strategy
) {
  for (const event of networkEvents) {
    const eventKey = `${event.chain}-${event.factory_address}`;
    const fromBlock = lastProcessedBlocks[`${eventKey}-${strategy}`]
      ? lastProcessedBlocks[`${eventKey}-${strategy}`] + 1
      : latestBlock;

    if (fromBlock > latestBlock) {
      continue;
    }

    log(
      `Processing ${event.chain} - ${event.exchange_name} | Strategy: ${strategy} | Blocks: ${fromBlock} to ${latestBlock}`
    );

    const logs = await getLogs(
      RPC_URL,
      event.factory_address,
      getEventTopic(event.event),
      fromBlock,
      latestBlock
    );

    if (logs.length > 0) {
      log(
        `Found ${logs.length} logs for ${event.factory_address} on ${event.chain} using ${strategy}`
      );
      for (const logEntry of logs) {
        const logData = {
          transaction_index: parseInt(logEntry.transactionIndex, 16),
          transaction_hash: logEntry.transactionHash,
          log_index: parseInt(logEntry.logIndex, 16),
          removed: logEntry.removed,
          block_number: parseInt(logEntry.blockNumber, 16),
          block_hash: logEntry.blockHash,
          exchange: event.exchange_name,
          network: event.chain,
          strategy: strategy,
        };

        // Insert data into Supabase
        const { data, error } = await supabase.from("logs").insert(logData);

        if (error) {
          log("Error inserting data into Supabase: " + error.message);
        } else {
          log(`Inserted log into Supabase: ${JSON.stringify(logData)}`);
        }
      }
    }

    // Update the last processed block for this event and strategy, regardless of whether logs were found
    lastProcessedBlocks[`${eventKey}-${strategy}`] = latestBlock;
  }
}

function getEventTopic(eventName) {
  switch (eventName) {
    case "PairCreated":
      return "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
    case "PoolCreated":
      return "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
    default:
      throw new Error(`Unsupported event: ${eventName}`);
  }
}

async function runContinuously() {
  while (true) {
    try {
      await processLogs();
    } catch (error) {
      log("Error in continuous execution: " + error.message);
    }
  }
}

// Initialize data and start continuous execution
initializeData().then(() => {
  runContinuously();
  log("Fetching logs. Press Ctrl+C to stop.");
});
