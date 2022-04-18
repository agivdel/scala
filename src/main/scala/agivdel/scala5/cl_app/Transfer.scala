//package agivdel.scala5.cl_app
//
//import com.sun.org.slf4j.internal.LoggerFactory
//import org.json4s.*
//import org.json4s.JsonAST.JValue
//import org.json4s.jackson.JsonMethods.*
//import org.slf4j.LoggerFactory
//import scalaj.http.*
//
//import java.time.LocalDate
//import scala.collection.immutable.Map
//import scala.collection.mutable.LinkedHashMap
//import scala.collection.{Map, MapFactory, mutable}
//import scala.util.control.Breaks.*
//
//object Transfer {
//  /**
//   * Метод проверяет номера рекламных кампаний на корректность через API Яндекса.<br>
//   * Каждый номер кампании проверяется только если ему соответствует ИСТИНА (т.е. в UI указано, что проверка нужна)
//   */
//  def isCampaignIdValid(ids: scala.collection.Map[Int, Boolean], token: String, clientLogin: String): scala.collection.Map[Int, Boolean] = {
//    val result: mutable.LinkedHashMap[Int, Boolean] = mutable.LinkedHashMap() //важно отдать пары значений в том же порядке, в котором они были получены
//    for ((id, isNeedValid) <- ids)
//      result += (id -> (isNeedValid && isCampaignIdValid(id, token, clientLogin)))
//    result
//  }
//
//  /**
//   * Метод проверяет номер рекламной кампании на корректность через API Яндекса.<br>
//   * Если номер корректен, он будет продублирован в ответе.
//   * Т.к. в запросе всегда только один номер, для корректного номера последняя строка ответа будет выглядеть "Total rows: 1".<br>
//   * Для некорректного номера ответ будет: "Total rows: 0".
//   */
//  private def isCampaignIdValid(id: Int, token: String, clientLogin: String) = {
//    val sc = s"\"Filter\":[{\"Field\":\"CampaignId\",\"Operator\":\"IN\",\"Values\":[\"$id\"]}]"
//    val params = s"{\"params\":{\"SelectionCriteria\":{$sc},\"FieldNames\":[\"CampaignId\"],\"ReportName\":\"Report\",\"ReportType\":\"CAMPAIGN_PERFORMANCE_REPORT\",\"DateRangeType\":\"AUTO\",\"Format\":\"TSV\",\"IncludeVAT\":\"NO\",\"IncludeDiscount\":\"NO\"}}"
//    val validRequest = Http("https://api.direct.yandex.com/json/v5/reports")
//      .postData(params)
//      .header("Accept", "application/json")
//      .header("Authorization", "Bearer " + token)
//      .header("Client-login", clientLogin)
//      .header("Accept-Language", "ru")
//      .header("skipReportHeader", "true") //чтобы в tsv не было первой строки (с заголовком)
//      .header("returnMoneyInMicros", "false")
//    val validResponseBody = validRequest.asString.body
//    val rows = validResponseBody.split("\n")
//    // В последней строке (со статистикой) число возвращенных строк кампаний начинается с индекса 12 (типа "Total rows: 1")
//    val rowsNumber = rows(rows.length - 1).substring(12)
//    rowsNumber.toInt == 1
//  }
//
//  def doTransfer(): Unit = {
//    val logger = org.slf4j.LoggerFactory.getLogger(getClass.getSimpleName)
//    logger.info("the transfer is starting")
//
//    val token = "AQAAAABdSrucAAevhY-Z9S5jyUpDln4kr3hXt48"
//    val clientLogin = "lunda.sk@yandex.ru"
//
//    val from = LocalDate.now().minusDays(1)
//    val to = LocalDate.now().minusDays(1)
//    val sc = s"\"DateFrom\":\"$from\",\"DateTo\":\"$to\""
//    val fn = "\"Date\",\"Cost\",\"Impressions\",\"Clicks\",\"CampaignName\",\"CampaignId\""
//    val params = s"{\"params\":{\"SelectionCriteria\":{$sc},\"FieldNames\":[$fn],\"ReportName\":\"Report\",\"ReportType\":\"CAMPAIGN_PERFORMANCE_REPORT\",\"DateRangeType\":\"CUSTOM_DATE\",\"Format\":\"TSV\",\"IncludeVAT\":\"NO\",\"IncludeDiscount\":\"NO\"}}"
//
//    val downloadRequest = Http("https://api.direct.yandex.com/json/v5/reports")
//      .postData(params)
//      .header("Accept", "application/json")
//      .header("Authorization", s"Bearer $token")
//      .header("Client-login", clientLogin)
//      .header("Accept-Language", "ru")
//      .header("skipReportHeader", "true") //чтобы в tsv не было первой строки (с заголовком)
//      .header("skipReportSummary", "true") //чтобы в tsv не было первой строки (с заголовком)
//      .header("returnMoneyInMicros", "false")
//
//    var yandexData = ""
//    breakable {
//      while (true) {
//        val downloadResponse = downloadRequest.asString
//
//        val pause = 15000
//        downloadResponse.code match
//          case 200 => logger.info("отчет Яндекс создан успешно"); yandexData = downloadResponse.body; break()
//          case 201 => logger.info(s"отчет Яндекс успешно поставлен в очередь в режиме офлайн. Повторная отправка запроса через $pause сек.")
//          case 202 => logger.info(s"отчет Яндекс формируется в режиме офлайн. Повторная отправка запроса через $pause сек.")
//          case 400 => logger.warn("параметры запроса в Яндекс указаны неверно или достигнут лимит отчетов в очереди"); break()
//          case 500 => logger.error("при формировании отчета Яндекс произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее"); break()
//          case 502 => logger.error("время формирования отчета Яндекс превысило серверное ограничение.\n\"Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных"); ""
//          case _ => logger.error("произошла непредвиденная ошибка"); break()
//      }
//    }
//
//    //получим данные из ответа Яндекса и преобразуем их
//    val rowsFromYandex: Array[String] = yandexData.split("\n")
//    val rowsForGoogle = Array.empty[String]
//    rowsForGoogle :+ "ga:date\tga:medium\tga:source\tga:adClicks\tga:adCost\tga:impressions\tga:campaign\tga:adwordsCampaignID"
//
//    for (i <- rowsFromYandex.indices) {
//      val oldColumns = rowsFromYandex(i).split("\t")
//      var newRow = ""
//      newRow += oldColumns(0).replace("-", "") + "\t"
//      newRow += "cpc" + "\t"
//      newRow += "yandex" + "\t"
//      newRow += oldColumns(3) + "\t"
//      newRow += oldColumns(1) + "\t"
//      newRow += oldColumns(2) + "\t"
//      newRow += oldColumns(4) + "\t"
//      newRow += oldColumns(5)
//      rowsForGoogle :+ newRow
//    }
//
//    val clientEmail = "client_email"
//    val tokenUri = "https://api.direct.yandex.com/json/v5/reports"
//    val privateKey = "private_key"
//
//    val tokenRequest = Http(tokenUri)
//      .postData(params)
//      .header("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
//      .header("assertion", s"Bearer $token")
//      .header("Content-Type", "application/x-www-form-urlencoded")
//    val tokenResponse = tokenRequest.asString
//    if (tokenResponse.code != 200) println("ошибка при получении токена аутентификации Google")
//    else println("получен токен аутентификации Google")
//    //конец авторизации Google
//
//    //подготовим файл для загрузки
//
//    //начало загрузки
//    val accountId = ""
//    val webPropertyId = ""
//    val customDataSourceId = ""
//    val uri = s"https://www.googleapis.com/upload/analytics/v3/management/accounts/$accountId/webproperties/$webPropertyId/customDataSources/$customDataSourceId/uploads"
//    val uploadRequest = Http(uri)
//      .header("Authorization", s"Bearer $token")
//      .header("Content-Type", "application/octet-stream")
//    val uploadResponse = uploadRequest.asString
//    if (uploadResponse.code != 200) println("ошибка при загрузке файла в Google")
//    else println(" данные переданы в Google ")
//  }
//}