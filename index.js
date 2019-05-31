const path = require("path");
const fs = require("fs").promises;

const argparse = require("argparse");
const mkdirp = require("mkdirp").sync;
const tqdm = require("ntqdm");
const request = require("request-promise-native");

const {ArborParser, SynapseClustering} = require("./arbor.js");

const {version} = require('../package.json');

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
parser.addArgument(["-o", "--outdir"], {
  help: 'Directory in which to dump output (one file per input, named {skeleton_id}.json)',
  default: "./"
});
parser.addArgument(["-f", "--fraction"], {
  help: "Fraction parameter for split finding algorithm",
  type: Number,
  default: DEFAULT_FRACTION
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

const parsedArgs = parser.parseArgs();

const credentials = !!parsedArgs.credentials ? require(parsedArgs.credentials) : {};
for (let key of ["user", "password", "token", "address"]) {
  if (!parsedArgs[key] && !!credentials[key]) {
    parsedArgs[key] = credentials[key];
  }
}

const doing = []

for (let item of tqdm(parsedArgs.skeletons, {desc: "queuing"})) {
  let p;
  if (item.toLowerCase().endswith(".json")) {
    p = readResponse(item);
  } else {
    p = fetchResponse(Number(item), parsedArgs.address, parsedArgs.token, parsedArgs.user, parsedArgs.pass);
  }

  p = handleCompactArbor(p, parsedArgs.fraction);
  p = writeOutObj(p, parsedArgs.outdir)
  doing.push(p)
}

watchProcessing(doing)

async function watchProcessing(doing) {
  for (let item of tqdm(doing, {desc: "processing"})) {
    await item;
  }
}

async function readResponse(fpath) {
  return JSON.parse(await fs.readFile(fpath));
}

async function fetchResponse(skid,  address, token, user, pass) {
  const uri = address;  // todo
  const options = {method: "GET", uri: uri, json: true};
  if (!!user && !!pass) {
    options.auth = {user, pass};
  }
  if (!!token) {
    options.headers = {"X-Authorization": "Token " + token}
  }
  return request(options);
}

async function writeOutObj(outObjPromise, outDir) {
  const outObj = await outObjPromise;
  const skid = outObj.compact_arbor[0][0]
  return fs.writeFile(path.join(outDir, `${skid}.json`), JSON.stringify(outObj));
}

async function handleCompactArbor(objPromise, fraction = DEFAULT_FRACTION) {
  const obj = await objPromise;
  const ap = ArborParser();
  ap.init("compact-arbor", obj);

  const headers = [
    [
      "skeleton_id", "this_treenode_id", "parent_id", "user_id",
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
