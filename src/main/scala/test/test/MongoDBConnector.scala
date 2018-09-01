package test.test

import scala.annotation.implicitNotFound
import org.bson.types.ObjectId
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.commons.MongoDBObject
import play.api.libs.json.Json
import com.mongodb.BasicDBObject
import com.mongodb.casbah.MongoCollection


class MongoDBConnector{
  var mongoClient: MongoClient = _
  /**
   * creates connection
   *
   * @param dbAddress is database address of mongodb
   * @param port is port at which mongodb is running
   *
   * @return Unit
   */
  def connect(dbAddress: String, port: String) {
    mongoClient = MongoClient(dbAddress, port.toInt)
  }

  def getRecordsById(dbName: String, collectionName: String, objId: String): String = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)
    val objectId: ObjectId = new ObjectId(objId)
    val mongoDbObj = MongoDBObject("_id" -> objectId)
    val result = coll.findOne(mongoDbObj)
    result.getOrElse().toString
  }

  def getRecordsByKey(dbName: String, collectionName: String, key: String, value: String): String = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)
    val mongoDbObj = MongoDBObject(key -> value)
    val result = coll.findOne(mongoDbObj)
    result.getOrElse().toString
  }

  def updateAcurianStagingRecord(dbName: String, collectionName: String, kv: Map[String, Any], patientID: String, id: String) = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)

    //val objectId: ObjectId = new ObjectId(objId)
    val query = MongoDBObject("id" -> patientID, "idd" -> id)

    val builder = MongoDBObject.newBuilder
    kv.foreach(p => {
      builder += p._1 -> { if (p._2 != null) p._2.toString else p._2 }
    })

    val newObj = builder.result
    coll.update(query, newObj, true)
  }

  def getRecordsByKey(dbName: String, collectionName: String, key: String, value: String, keySource: String, valueSource: String): String = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)
    val mongoDbObj = MongoDBObject(key -> value, keySource -> valueSource)
    val result = coll.findOne(mongoDbObj)
    result.getOrElse().toString

  }

  def getCollection(dbName: String, collectionName: String): MongoCollection = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    db(collectionName)
  }

  def saveRecord(dbName: String, collectionName: String, kv: collection.mutable.Map[String, Any]): String = {
    val db: MongoDB = mongoClient(dbName)
    val collection = db(collectionName)
    val recordBuilder = MongoDBObject.newBuilder
    kv.foreach(pair => {
      recordBuilder += pair._1 -> pair._2
    })

    val record = recordBuilder.result
    collection.insert(record)

    val id = record.get("_id")
    return id.toString
  }

  def updateRecord(dbName: String, collectionName: String, kv: collection.mutable.Map[String, Any], objId: String) = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)

    val objectId: ObjectId = new ObjectId(objId)
    val query = MongoDBObject("_id" -> objectId)

    val builder = MongoDBObject.newBuilder
    kv.foreach(p => {
      builder += p._1 -> p._2
    })

    val newObj = builder.result
    coll.update(query, newObj)
  }

  def updateRecordValue(dbName: String, collectionName: String, searchKey: String, searchValue: String, replaceKey: String, replaceValue: Boolean) = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)

    var query = new BasicDBObject(searchKey, searchValue)
    var update = new BasicDBObject
    update.put("$set", new BasicDBObject(replaceKey, replaceValue))
    coll.update(query, update)
  }

  def deleteRecord(dbName: String, collectionName: String, key: String, value: String) = {
    val db: com.mongodb.casbah.MongoDB = mongoClient(dbName)
    val coll = db(collectionName)
    val query = MongoDBObject(key -> value)
    coll.findAndRemove(query)
  }


  def closeMongoClient() {
    if (mongoClient != null) {
      mongoClient.close
      mongoClient = null
    }
  }
}