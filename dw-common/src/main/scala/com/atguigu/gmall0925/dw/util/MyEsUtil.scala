package com.atguigu.gmall0925.dw.util

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core._



object MyEsUtil {
  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def main(args: Array[String]): Unit = {
        val client: JestClient = getClient
       val query="{\n  \"query\": {\n    \"match\": {\n      \"mid\": \"102\"\n    }\n  }\n}";
    //查询
       val search: Search = new Search.Builder(query).addIndex("gmall0808_dau").addType("_doc").build()

       val result: SearchResult = client.execute(search)

        val list: util.List[SearchResult#Hit[util.HashMap[String, String], Void]] = result.getHits(classOf[util.HashMap[String,String]])
        import collection.JavaConversions._
        for (hit:SearchResult#Hit[util.HashMap[String, String], Void] <- list ) {
          val source: util.HashMap[String, String] = hit.source
          println(source.toString)
        }
   //单值保存
//    val source="{\n  \"mid\":\"103\",\n   \"uid\":\"9999\"\n}"
//    val index: Index = new Index.Builder(source).index("gmall0808_dau").`type`("_doc").build()
//    client.execute(index)

  }

//批量操作
  def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={
    val bulkbuilder = new  Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for (doc <- list ) {
      val index: Index = new Index.Builder(doc).build()
      bulkbuilder.addAction(index)
    }
    val client: JestClient = getClient
    val result: BulkResult = client.execute(bulkbuilder.build())
    println("保存："+result.getItems.size()+"条")
    close(client)
  }



}
