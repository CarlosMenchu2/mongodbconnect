const {MongoClient} = require('mongodb');

async function main() {

    const uri = '';

    const client = new MongoClient(uri,{ useNewUrlParser: true, useUnifiedTopology: true });

    try {
        //Connect to MongoDB Cluster
        await client.connect();
       

        const pipeline = [
            {
                '$match': {
                    'operationType':'insert'
                }
            }
        ]

        await monitorListEventEmiter(client,15000,pipeline);
        
    } finally {
        
        await client.close();
    }
}

main().catch(console.error);

async function monitorListEventEmiter(client, time=60000, pipeline = []) {

    const collection = client.db("arbinb").collection("user");
    
    const changeStream = collection.watch(pipeline);
   
    changeStream.on('change', (next) =>{
         
        console.log(next);
        secuence(next,client);
   });

    await closeChangeSteams(time,changeStream);

}

function closeChangeSteams(time = 60000, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() =>{
            console.log('Closing the change stream');
            changeStream.close();
            resolve();
        }, time)
    })
}

async function secuence(changeEvent, client) {
    //var docId = changeEvent.fullDocument._id;
    var docId = changeEvent.documentKey._id;
    
    
    const usercollection = client.db(changeEvent.ns.db).collection(changeEvent.ns.coll);
    const countercollection = client.db("arbinb").collection("counters");

    var counter = await countercollection.findOneAndUpdate({_id: changeEvent.ns },{ $inc: { seq_value: 1 }}, { returnNewDocument: true, upsert : true});
    var updateRes = await usercollection.updateOne({_id : docId},{ $set : {secuencia : counter.value.seq_value}});
    
    console.log(`Updated ${JSON.stringify(changeEvent.ns)} with counter ${counter.value.seq_value} result : ${JSON.stringify(updateRes)}`);
};