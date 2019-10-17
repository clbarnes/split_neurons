const path = require("path");
const fs = require("fs").promises;

const argparse = require("argparse");
const mkdirp = require("mkdirp").sync;
const tqdm = require("ntqdm");
// const request = require("request-promise-native");
const urljoin = require("url-join");
const RateLimiter = require("request-rate-limiter");

const {ArborParser, SynapseClustering} = require("./arbor.js");

const {version} = require('./package.json');

const parser = new argparse.ArgumentParser({
  version: version,
  addHelp: true,
  description: "Tool to find the split points of many arbors"
});

DEFAULT_FRACTION = 0.9

parser.addArgument("skeletons", {
  help: 'Either skeleton IDs, or paths to JSON files containing compact-arbor responses for those skeleton IDs (must end with .json)',
  nargs: argparse.Const.ZERO_OR_MORE
});
parser.addArgument(["-j", "--json"], {
  help: "Read skeleton IDs from a JSON file saved from the skeleton selection table"
})
parser.addArgument(["-o", "--outdir"], {
  help: 'Directory in which to dump output (one file per input, named {skeleton_id}.json)',
  defaultValue: "./",
  type: path.resolve
});
parser.addArgument(["-f", "--fraction"], {
  help: "Fraction parameter for split finding algorithm",
  type: Number,
  defaultValue: DEFAULT_FRACTION
});
parser.addArgument(["-u", "--user"], {
  help: "Username for HTTP auth if required by the CATMAID server",
});
parser.addArgument(["-p", "--password"], {
  help: "Password for HTTP auth if required by the CATMAID server",
});
parser.addArgument(["-t", "--token"], {
  help: "Auth token if required by the CATMAID instance"
});
parser.addArgument(["-a", "--address"], {
  help: "Root URL of the CATMAID instance (e.g. https://neurocean.janelia.org/catmaidL1/)"
});
parser.addArgument(["-c", "--credentials"], {
  help: 'JSON file containing "address", "token", "user", and "password" fields instead of passing them separately'
})
parser.addArgument(["-i", "--projectId"], {
  help: "Project ID to which the skeletons belong"
})

const parsedArgs = parser.parseArgs();

async function main(parsedArgs) {
  const credentials = !!parsedArgs.credentials ? require(path.resolve(parsedArgs.credentials)) : {};
  for (let key of ["user", "password", "token", "address", "projectId"]) {
    if (!parsedArgs[key] && !!credentials[key]) {
      parsedArgs[key] = credentials[key];
    }
  }

  if (!!parsedArgs.json) {
    const other = JSON.parse(await fs.readFile(parsedArgs.json));
    parsedArgs.skeletons = parsedArgs.skeletons.concat(other.map(o => '' + o.skeleton_id));
  }

  mkdirp(parsedArgs.outdir);

  const doing = [];

  for (let item of tqdm(parsedArgs.skeletons, {desc: "queuing"})) {
    let p;
    let skid;
    if (item.toLowerCase().endsWith(".json")) {
      skid = Number(path.basename(item, ".json"))
      p = readResponse(item);
    } else {
      skid = Number(item)
      p = fetchResponse(skid, parsedArgs.address, parsedArgs.projectId, parsedArgs.token, parsedArgs.user, parsedArgs.password);
    }

    p = handleCompactArbor(p, parsedArgs.fraction);
    p = writeOutObj(skid, p, parsedArgs.outdir)
    doing.push(p)
  }

  await watchProcessing(doing)
}

async function watchProcessing(doing) {
  for (let item of tqdm(doing, {desc: "processing"})) {
    await item;
  }
}

async function readResponse(fpath) {
  return JSON.parse(await fs.readFile(fpath));
}

const limiter = new RateLimiter({
  rate: 4,
  interval: 1,
  backoffCode: 502,
  backoffTime: 5,
  maxWaitingTime: Infinity
});

async function fetchResponse(skid,  address, projectId, token, user, pass) {
  const uri = urljoin(address, ''+projectId, ''+skid, ''+1, ''+1, ''+0, "compact-arbor");
  const options = {method: "GET", uri: uri};
  if (!!user && !!pass) {
    options.auth = {user, pass};
  }
  if (!!token) {
    options.headers = {"X-Authorization": "Token " + token}
  }
  const response = await limiter.request(options);
  return JSON.parse(response.body)
}

async function writeOutObj(skid, outObjPromise, outDir) {
  const outObj = await outObjPromise;
  return fs.writeFile(path.join(outDir, `${skid}.json`), JSON.stringify(outObj, null, 2));
}

const relationIds = {
  "0": "presynaptic",
  "1": "postsynaptic",
  "2": "gap junction",
  "-1": "other"
}

async function handleCompactArbor(objPromise, fraction = DEFAULT_FRACTION) {
  const obj = await objPromise;
  const ap = new ArborParser();
  ap.init("compact-arbor", obj);

  const headers = [
    [
      "this_treenode_id", "parent_id", "user_id",
      "location_x", "location_y", "location_z",
      "radius", "confidence"
    ],
    [
      "this_treenode_id", "this_confidence",
      "connector_id", "that_confidence",
      "that_treenode_id", "that_skeleton_id",
      "this_relation_id", "that_relation_id"
    ]
  ];

  const outObj = {
    compact_arbor: obj,
    compact_arbor_headers: headers,
    fraction: fraction,
    relation_ids: relationIds,
    cut: null
  }

  positions = ap.positions;

  const fc = ap.arbor.flowCentrality(
    ap.outputs,
    ap.inputs,
    ap.n_outputs,
    ap.n_inputs,
  );

  if (fc !== null) {
    const regions = SynapseClustering.prototype.findArborRegions(
      ap.arbor,
      fc,
      fraction,
    );

    if (regions !== null) {
      outObj.cut = SynapseClustering.prototype.findAxonCut(
        ap.arbor,
        ap.outputs,
        regions.above,
        positions,
      );
    }
  }

  return outObj;
}

main(parsedArgs);
