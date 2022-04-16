

import java.time.LocalDate

import scala.collection.{Map, MapFactory, mutable}
import scala.collection.mutable.LinkedHashMap
import scalaj.http
import scalaj.http.{Http, HttpRequest}

class Y2GTransfer {
  /**
   * Метод проверяет номера рекламных кампаний на корректность через API Яндекса.<br>
   * Каждый номер кампании проверяется только если ему соответствует ИСТИНА (т.е. в UI указано, что проверка нужна)
   */
  def isCampaignIdValid(ids: Map[Int, Boolean], token: String, clientLogin: String): Map[Int, Boolean] = {
    val result: mutable.LinkedHashMap[Int, Boolean] = mutable.LinkedHashMap() //важно отдать пары значений в том же порядке, в котором они были получены
    for ((id, isNeedValid) <- ids)
      result += (id -> (isNeedValid && isCampaignIdValid(id, token, clientLogin)))
    result
  }

  /**
   * Метод проверяет номер рекламной кампании на корректность через API Яндекса.<br>
   * Если номер корректен, он будет продублирован в ответе.
   * Т.к. в запросе всегда только один номер, для корректного номера последняя строка ответа будет выглядеть "Total rows: 1".<br>
   * Для некорректного номера ответ будет: "Total rows: 0".
   */
  private def isCampaignIdValid(id: Int, token: String, clientLogin: String) = {
    val sc = s"\"Filter\":[{\"Field\":\"CampaignId\",\"Operator\":\"IN\",\"Values\":[\"$id\"]}]"
    val params = s"{\"params\":{\"SelectionCriteria\":{$sc},\"FieldNames\":[\"CampaignId\"],\"ReportName\":\"Report\",\"ReportType\":\"CAMPAIGN_PERFORMANCE_REPORT\",\"DateRangeType\":\"AUTO\",\"Format\":\"TSV\",\"IncludeVAT\":\"NO\",\"IncludeDiscount\":\"NO\"}}"
    val validRequest = Http("https://api.direct.yandex.com/json/v5/reports")
      .postData(params)
      .header("Accept", "application/json")
      .header("Authorization", "Bearer " + token)
      .header("Client-login", clientLogin)
      .header("Accept-Language", "ru")
      .header("skipReportHeader", "true") //чтобы в tsv не было первой строки (с заголовком)
      .header("returnMoneyInMicros", "false")
    val validResponseBody = validRequest.asString.body
    val rows = validResponseBody.split("\n")
    // В последней строке (со статистикой) число возвращенных строк кампаний начинается с индекса 12 (типа "Total rows: 1")
    val rowsNumber = rows(rows.length - 1).substring(12)
    rowsNumber.toInt == 1
  }

  def doTransfer(): Unit = {
    val token = "afgfgdfb"
    val clientLogin = "sk.yandex@lunda.ru"

    val from = LocalDate.now().minusDays(1)
    val to = LocalDate.now().minusDays(1)
    val sc = s"\"DateFrom\":\"$from\",\"DateTo\":\"$to\""
    val fn = "\"Date\",\"Cost\",\"Impressions\",\"Clicks\",\"CampaignName\",\"CampaignId\""
    val params = s"{\"params\":{\"SelectionCriteria\":{$sc},\"FieldNames\":[$fn],\"ReportName\":\"Report\",\"ReportType\":\"CAMPAIGN_PERFORMANCE_REPORT\",\"DateRangeType\":\"CUSTOM_DATE\",\"Format\":\"TSV\",\"IncludeVAT\":\"NO\",\"IncludeDiscount\":\"NO\"}}"

    val downloadRequest = Http("https://api.direct.yandex.com/json/v5/reports")
      .postData(params)
      .header("Accept", "application/json")
      .header("Authorization", s"Bearer $token")
      .header("Client-login", clientLogin)
      .header("Accept-Language", "ru")
      .header("skipReportHeader", "true") //чтобы в tsv не было первой строки (с заголовком)
      .header("skipReportSummary", "true") //чтобы в tsv не было первой строки (с заголовком)
      .header("returnMoneyInMicros", "false")

    var yandexData = ""
    while (true) {
      val downloadResponse = downloadRequest.asString

      val pause = 15000
      yandexData = downloadResponse.code match
        case 200 => println("отчет Яндекс создан успешно"); downloadResponse.body
        case 201 => println(s"отчет Яндекс успешно поставлен в очередь в режиме офлайн. Повторная отправка запроса через $pause сек."); ""
        case 202 => println(s"отчет Яндекс формируется в режиме офлайн. Повторная отправка запроса через $pause сек."); ""
        case 400 => println("параметры запроса в Яндекс указаны неверно или достигнут лимит отчетов в очереди"); ""
        case 500 => println("при формировании отчета Яндекс произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее"); ""
        case 502 => println("время формирования отчета Яндекс превысило серверное ограничение.\n\"Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных"); ""
        case _ => println("произошла непредвиденная ошибка"); ""
    }

    //получим данные из ответа Яндекса и преобразуем их
    val rowsFromYandex: Array[String] = yandexData.split("\n")
    val rowsForGoogle = Array.empty[String]
    rowsForGoogle :+ "ga:date\tga:medium\tga:source\tga:adClicks\tga:adCost\tga:impressions\tga:campaign\tga:adwordsCampaignID"

    for (i <- rowsFromYandex.indices) {
      val oldColumns = rowsFromYandex(i).split("\t")
      val newRow = new StringJoiner("\t")
      newRow.add(oldColumns(0).replace("-", ""))
      newRow.add("cpc")
      newRow.add("yandex")
      newRow.add(oldColumns(3))
      newRow.add(oldColumns(1))
      newRow.add(oldColumns(2))
      newRow.add(oldColumns(4))
      newRow.add(oldColumns(5))
      rowsForGoogle :+ newRow.toString
    }

    val clientEmail = "client_email"
    val tokenUri = "https://api.direct.yandex.com/json/v5/reports"
    val privateKey = "private_key"

    val tokenRequest = Http(tokenUri)
      .postData(params)
      .header("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
      .header("assertion", s"Bearer $token")
      .header("Content-Type", "application/x-www-form-urlencoded")
    val tokenResponse = tokenRequest.asString
    if (tokenResponse.code != 200) println("ошибка при получении токена аутентификации Google")
    else println("получен токен аутентификации Google")
    //конец авторизации Google

    //подготовим файл для загрузки

    //начало загрузки
    val accountId = ""
    val webPropertyId = ""
    val customDataSourceId = ""
    val uri = s"https://www.googleapis.com/upload/analytics/v3/management/accounts/$accountId/webproperties/$webPropertyId/customDataSources/$customDataSourceId/uploads"
    val uploadRequest = Http(uri)
      .header("Authorization", s"Bearer $token")
      .header("Content-Type", "application/octet-stream")
    val uploadResponse = uploadRequest.asString
    if (uploadResponse.code != 200) println("ошибка при загрузке файла в Google")
    else println(" данные переданы в Google ")
  }
}