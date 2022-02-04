


import fetch from 'node-fetch';
import loki from 'lokijs';
import fs from 'fs';
import { create } from 'ipfs-http-client'
import all from 'it-all'
import { concat } from 'uint8arrays/concat'
import tar from 'tar';
import dir from 'node-dir';

import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'

let arg;

// const ipfs = create(new URL('https://ipfs.io/api/v0/'))
const ipfs = create(new URL('http://localhost:8080/api/v0/'));



const argv = yargs(hideBin(process.argv))
  .command('inspect', 'Inspects a particular OBJKT metadata', {
    id: {
      description: 'the OBJKT ID to check for',
      type: 'number',
      alias: 'i',
      nargs: 1,
      demandOption: true
    }
  })
  .command('fetchFiles', 'Fetch OBJTKs not yet fetched before', {
  })
  .command('fetchMetadata', 'Add new OBJTK metadata from hicdex', {
  })
  .command('listFailed', 'List of OBJKTs failed to retrieve', {
  })
  .command('validateFS', 'Check that all retrieved OBJKTs have a valid file in the FS', {
  })
  .command('findUncleanDirs', 'Check that all retrieved OBJKTs have a valid file in the FS', {
  })
  .command('extractExternalLinks', 'Extract metadata about external links.', {
  })
  .command('extract', 'Extracts a particular OBJKT. Or all if ID not provided', {
    id: {
      description: 'the OBJKT ID to extract',
      type: 'number',
      alias: 'i',
      nargs: 1,
    }
  })
  .command('cleanSecPolMeta', 'remove Content-Security-Policy meta tag recursively.', {
  })
  .demandCommand(1, 'Command Required.')
  .strict()
  .help('h')
  .alias('help', 'h').argv;


function setup() {

    arg = process.argv.slice(2);

    db = new loki('./data/db/dnft.db', {
         autoload: true,
         autoloadCallback : databaseInitialize,
         autosave: true, 
         autosaveInterval: 1000 // save every 1 second
     });
 }

 async function main() {

    switch(argv._[0]) {
        case 'inspect':
            queryItem(argv.id);
            break;
        
        case 'fetchMetadata':
            // fetch new OBJKTs since last recorded offset
            await fetchOBJKTmetadata();
            break;

        case 'fetchFiles':
            await fetchFiles();
            break;

        case 'listFailed':
            listFailed();
            break;

        case 'validateFS':
            validateFS();
            break;

        case 'findUncleanDirs':
            findUncleanDirs();
            break;

        case 'extract':
            extract(argv.id);
            break;

        case 'cleanSecPolMeta':
            await cleanSecPolMeta();
            break;

        case 'extractExternalLinks':

            db = new loki('./data/db/dnft.db', {
                autoload: true,
                autoloadCallback : extractExternalLinks,
                autosave: true, 
                autosaveInterval: 1000 // save every 1 second
            });

            break;
    }

    db.close();

    console.log("Done.");
}
 

const skipList = [

                84333,
                212395,
                357171,
                357181,
                573214,
                573216,
                573218,
                573222,
                573224,
                603951,
                603952,
                603962
            
            ];



const BASE_FS_DIR = './data/fs/';

let db;

let dbConfig;
let dbObjkts;

const HEN_OFFSET = 0;
const PAGE_SIZE = 1000;

const HEN_GRAPHQL_URL = 'https://api.hicdex.com/v1/graphql';

const query = `
    query XDirectoryOBJKTs($offset: Int = 0, $limit: Int = 10) {
        hic_et_nunc_token(where: {_or: [ {mime: {_eq: "application/x-directory"}}, {mime: {_eq: "image/svg+xml"}}], supply: {_gt: "0"}}, limit: $limit, order_by: {id: asc}, offset: $offset) {
            id
            supply
            timestamp
            title
            artifact_uri
            metadata
            creator {
                address
                name
            }
            mime
            description
            token_tags {
                tag {
                tag
                }
            }
            }
        }
`;

async function fetchGraphQL(operationsDoc, operationName, variables) {
    const result = await fetch(HEN_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        query: operationsDoc,
        variables: variables,
        operationName: operationName
      }),
    });
  
    return await result.json();
  }

  async function fetchOBJKTmetadata() {

    let hen_config = dbConfig.findOne({ type:'hen' });

    if(!hen_config) {
        hen_config = dbConfig.insert({
            type: 'hen',
            offset: HEN_OFFSET
        });
    }

    let offset = hen_config.offset;

    let out = [];

    let done = false;
    let pageSize = PAGE_SIZE;

    while(!done) {
        const { errors, data } = await fetchGraphQL(query, "XDirectoryOBJKTs", {"limit": pageSize, "offset": offset});

        if (errors) {
            console.error(errors);
        }

        const result = data.hic_et_nunc_token

        if (result.length > 0) {
            console.log("Adding", result.length, "new dyn OBJKTs.");

            for (let i = 0; i < result.length; i++) {
                result[i].type = "hen";
                dbObjkts.insert(result[i]);
            }

            offset += result.length;
        } else {
            done = true;
        }

    }

    hen_config.offset = offset;
    dbConfig.update(hen_config);
  }


function queryItem(rid) {

    let record = dbObjkts.findOne({ id: rid });
    
    if(!record) {
        console.log("No record with id", rid, "found");
    } else {
        console.log(record);
    }

}


function findUncleanDirs() {

    let records = dbObjkts.find({ isFSFetched: {'$eq': true}  });

    console.log("Validating cleaningness of", records.length, "directories...");

    let errors = 0;

    let completionSteps = Math.floor(records.length / 10);
    let counter = 0;
    let percent = 0;
    let nextCompletionMilestone = counter + completionSteps;

    for (let i = 0; i < records.length; i++) {

        let objktMeta = records[i];

        let dir = BASE_FS_DIR + "hen/" + objktMeta.id;
        let filename = '/OBJKT_' + objktMeta.id + '.tar.gz';
        let path = dir + filename;


        fs.readdirSync(dir).forEach(file => {
            let f = "/"+file;
            if(f != filename) {
                console.log("Found unknown file", f);
                console.log("Expected:", filename)
            }
          });

        counter++;
        if (counter == nextCompletionMilestone ) {
            percent += 10;
            console.log("" + percent + "%..." );
            nextCompletionMilestone += completionSteps;
        }
    }

    console.log("Errors:", errors);

    console.log("Total valid records:", records.length);
}

function listFailed() {
    let records = dbObjkts.find({ isFSFetched: {'$eq': false}, isFSError: {'$eq': true}  });

    for (let i = 0; i < records.length; i++) {

        let objktMeta = records[i];

        let dir = BASE_FS_DIR + "hen/" + objktMeta.id;
        let filename = '/OBJKT_' + objktMeta.id + '.tar.gz';
        let path = dir + filename;

        try {
            if ( !(fs.readFileSync(path).length === 0) ) {
                console.error("OBJKT", objktMeta.id, "File for", path, "is NOT empty!");
                console.log(fs.readFileSync(path).length);
            }
        }
        catch (err) {
            // expected
        }

        console.log(objktMeta.id);
    }

    console.log("Total failed records:", records.length);
    

}

function extract(rid) {

    let records;

    let errors = 0;

    if(rid) {
        records = [];
        records.push(dbObjkts.findOne({ id: rid }));
    } else {
        records = dbObjkts.find({ isFSFetched: {'$eq': true}  , isFSError: {'$ne': true}  });
    }

    for(let i = 0; i < records.length; i++) {

        let objktMeta = records[i];

        let dir = BASE_FS_DIR + "hen/" + objktMeta.id;
        let filename = '/OBJKT_' + objktMeta.id + '.tar.gz';
        let path = dir + filename;
        let extractionDir = dir + '/extracted/';

        console.log("Extracting OBJKT", objktMeta.id, "...");

        // create directory
        if (!fs.existsSync(extractionDir)){
            fs.mkdirSync(extractionDir);
        }

        try {
            tar.extract(
                {
                file: path,
                C: extractionDir,
                sync: true
                }
            ).then(_=> {
                renameHashToIndex(objktMeta);
            })
        }
        catch (err) {
            console.error("Error extracting OBJKT", objktMeta.id);
            errors++;

            // try to rename anyway
            renameHashToIndex(objktMeta);
        }
    }

    console.log("Errors during extraction:", errors);

}

function renameHashToIndex(objktMeta) {
    try {

        let dir = BASE_FS_DIR + "hen/" + objktMeta.id;
        let extractionDir = dir + '/extracted/';

        // get cid
        let cid = objktMeta.artifact_uri;

        // remove ipfs prefix
        if(cid.startsWith('ipfs://') ) {
            cid = cid.slice(7);
        }

        let mainFilename = extractionDir + cid
        let newFilename = extractionDir + 'index.html';

        // rename main cid to index.html
        fs.rename(mainFilename, newFilename, function(err) {
            if ( err ) console.log('ERROR: ' + err);
        });
    }
    catch (err) {
        // ok to be silent here
        console.error("Error renaming file", err);
    }

}

function validateFS() {
    let records = dbObjkts.find({ isFSFetched: {'$eq': true}  });

    console.log("Validating presence of", records.length, "files...");

    let errors = 0;

    let completionSteps = Math.floor(records.length / 10);
    let counter = 0;
    let percent = 0;
    let nextCompletionMilestone = counter + completionSteps;

    for (let i = 0; i < records.length; i++) {

        let objktMeta = records[i];

        let dir = BASE_FS_DIR + "hen/" + objktMeta.id;
        let filename = '/OBJKT_' + objktMeta.id + '.tar.gz';
        let path = dir + filename;

        try {
            if ( fs.readFileSync(path).length > 0 )  {
                // expected
            } else {
                console.error("Found empty file", path);
                errors++;
            }
        }
        catch (err) {
            console.error("Error!", path);
            errors++;
        }

        counter++;
        if (counter == nextCompletionMilestone ) {
            percent += 10;
            console.log("" + percent + "%..." );
            nextCompletionMilestone += completionSteps;
        }
    }

    console.log("Errors:", errors);

    console.log("Total valid records:", records.length);
}



function resetDownloadFlags() {
    let resetList = [
        
    ];


        for(let i = 0; i < resetList.length; i++) {

            let rid = resetList[i];

            console.log(rid);

            let record = dbObjkts.findOne({ id: rid });

            record.isFSFetched = false;
            record.isFSError = false;

            dbObjkts.update(record);

            //console.log(record);
        }

}


async function fetchFiles() {

    // let records = dbObjkts.find({ id: {'$eq': 261081} } );

    let emptyFilesCount = 0;

    do {

        let records = dbObjkts.find({ isFSFetched: {'$ne': true}  , isFSError: {'$ne': true}  });
        let objktMeta;

        console.log("Records left to fetch:", records.length)

        emptyFilesCount = 0;

        for(let i = 0; i < records.length; i++) {
            objktMeta  = records[i];

            if( !skipList.includes(objktMeta.id) ) {  //(!objktMeta.isFSFetched || objktMeta.isFSFetched == false) && (!objktMeta.isFSError && objktMeta.isFSError == false)) {

                // fetch IPFS resources
                let cid = objktMeta.artifact_uri;

                // remove ipfs prefix
                if(cid.startsWith('ipfs://') ) {
                    cid = cid.slice(7);
                }

                // remove argument suffixes
                let p = cid.indexOf('?');
                if( p && p > 0) {
                    cid = cid.slice(0, p);
                }

                console.log("'Right-clicking and saving' OBJKT#", objktMeta.id, cid);

                try {

                    let data = concat(await all(ipfs.get(cid, {archive: true, compress: true, timeout: 30000})));

                    // create directory
                    let dir = BASE_FS_DIR + "hen/" + objktMeta.id;
                    if (!fs.existsSync(dir)){
                        fs.mkdirSync(dir);
                    }

                    let filename = '/OBJKT_' + objktMeta.id + '.tar.gz';

                    fs.writeFileSync(dir + filename, data);

                    // check if file is empty
                    if (fs.readFileSync(dir + filename).length === 0) {
                        console.error("File for", objktMeta.id, "is empty.");
                        emptyFilesCount++;
                    } else {
                        objktMeta.isFSFetched = true;
                        dbObjkts.update(objktMeta);
                    }

                // console.log("Fetched", objktMeta.id);
                }
                catch(err) {
                    console.error("Could not retrive/store IPFS content for OBJK ", objktMeta.id);
                    console.error(err);
                    objktMeta.isFSError = true;
                    dbObjkts.update(objktMeta);
                }
            }
        }

        console.log("Total Dyn OBJKTs: ", records.length);

        if(emptyFilesCount > 0) {
            console.log("Retrieved", emptyFilesCount, "empty files. You may want to re-try the fetch.");

        }

    } while (emptyFilesCount > 0);

}

function renameFiles() {

    let records = dbObjkts.find({ isFSFetched: {'$eq': true}   }); //  , isFSError: {'$ne': true}});
    let objktMeta;

    console.log("Files to rename:", records.length)


    for(let i = 0; i < records.length; i++) {
        objktMeta  = records[i];

        if( !skipList.includes(objktMeta.id) ) {

            let dir = BASE_FS_DIR + "hen/" + objktMeta.id;

            let oldName = dir + '/data.tgz';
            let newName = dir + '/OBJKT_' + objktMeta.id + '.tar.gz';

            console.log("Renaming", oldName, "to", newName);

            fs.rename(oldName, newName, function(err) {
                if ( err ) console.log('ERROR: ' + err);
            });

        }

    }
}


async function extractExternalLinks() {

    let records = dbObjkts.find({ isFSFetched: {'$eq': true}  });

    let errors = 0;

    let completionSteps = Math.floor(records.length / 10);
    let counter = 0;
    let percent = 0;
    let nextCompletionMilestone = counter + completionSteps;

    for (let i = 0; i < records.length; i++) {

        let objktMeta = records[i];

        let base_dir = BASE_FS_DIR + "hen/" + objktMeta.id;

        dir.paths(base_dir, function(err, paths) {
            if (err) throw err;
    
            for (let i = 0; i < paths.files.length; i++) {
                let filename = paths.files[i];
    
                if (filename.endsWith('.tar.gz')) {
                    continue;
                }
    
                extractExternalLinksFile(objktMeta, filename);
            }
        });

        counter++;
        if (counter == nextCompletionMilestone ) {
            percent ++;
            console.log("" + percent + "%..." );
            nextCompletionMilestone += completionSteps;
        }
    }

    console.log("Errors:", errors);

    console.log("Total valid records:", records.length);

    db.close();
    console.log("Done.");
}

function extractExternalLinksFile(objktMeta, filename) {

    if(!objktMeta.externalLinks) {
        objktMeta.externalLinks = {};
    }

    const data = fs.readFileSync(filename, {encoding:'utf8', flag:'r'});

    for (let i = 0; i < externalDomains.length; i++) {

        let domain = externalDomains[i];

        if (data.includes(domain)) {
            objktMeta.externalLinks[domain] = true;
            console.log("found", domain, "in file", filename);
        }
    }

    dbObjkts.update(objktMeta);
}

async function cleanSecPolMeta() {

    console.log("Removing security meta tags, this may take a while...");

    let base_dir = BASE_FS_DIR + "hen/"

    console.log("Retrieving file list...");

    
    await dir.paths(base_dir, function(err, paths) {
        if (err) throw err;

        let completionSteps = Math.floor(paths.files.length / 100);
        let counter = 0;
        let percent = 0;
        let nextCompletionMilestone = counter + completionSteps;

        console.log("Starting clean operation...");

        for (let i = 0; i < paths.files.length; i++) {
            let filename = paths.files[i];

            if (filename.endsWith('.tar.gz')) {
                continue;
            }

            removeSecMetaTag(filename);

            counter++;
            if (counter == nextCompletionMilestone ) {
                percent ++;
                console.log("" + percent + "%..." );
                nextCompletionMilestone += completionSteps;
            }

        }

    });

}

function removeSecMetaTag(filename) {

    const data = fs.readFileSync(filename, {encoding:'utf8', flag:'r'});

    let tagStart = data.indexOf("<meta http-equiv=\"Content-Security-Policy");

    if (tagStart > 0) {
        let tagEnd = data.indexOf(">", tagStart);

        var a = data.substring(0, tagStart);
        var b = data.substring(tagEnd+1);

        var newData = a + b;

        fs.writeFileSync(filename, newData);
    }

}

function databaseInitialize() {

    dbConfig = db.getCollection("config");
    if (dbConfig === null) {
        dbConfig = db.addCollection("config");
    }

    dbObjkts = db.getCollection("hen_objkts");
    if (dbObjkts === null) {
        dbObjkts = db.addCollection("hen_objkts");
    }

    main();
}

let externalDomains = ['better-call.dev',
                        'cryptonomic-infra.tech',
                        'infura.io',
                        'infura-ipfs.io',
                        'bootstrap.libp2p.io',
                        'preload.ipfs.io',
                        'mainnet.smartpy.io',
                        'mainnet-tezos.giganode.io',
                        'api.etherscan.io',
                        'api.thegraph.com',
                        'tzkt.io',
                        'api.hicdex.com',
                        'hdapi.teztools.io',
                        'api.tzstats.com',
                        'wikidata.org',
                        'coinmarketcap.com',
                        'api.openweathermap.org',
                        'hicetnunc.xyz'];

setup();


