import com.google.gson.Gson
import com.google.gson.JsonObject
import com.gridnine.platform.common.lundaru.model.adstransfer.AdsTransferY2G
import com.gridnine.platform.common.lundaru.model.adstransfer.Campaign
import com.gridnine.platform.server.lundaru.adstransfer.AdsTransferMaintenanceJob
import com.gridnine.xtrip.common.model.Xeption
import com.gridnine.xtrip.common.model.entity.EntityStorage

import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import io.jsonwebtoken.impl.TextCodec
import org.apache.commons.lang.StringUtils
import org.apache.http.NameValuePair
import org.apache.http.auth.AuthenticationException
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.FileEntity
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.net.URISyntaxException
import java.nio.charset.StandardCharsets
import java.security.KeyFactory
import java.security.NoSuchAlgorithmException
import java.security.PrivateKey
import java.security.spec.InvalidKeySpecException
import java.security.spec.PKCS8EncodedKeySpec
import java.util
import java.util.*
import java.util.stream.Collectors

import scala.collection.Map

/**
 * Класс отвечает за подготовку данных для запросов и отправку запросов по процедуре обмена Яндекс-Google
 */
object Y2GTransfer {
  private val YANDEX_COMMON_PARAMS = "{\"params\":{\"SelectionCriteria\":{%s},\"FieldNames\":[%s],\"ReportName\":\"Report\",\"ReportType\":\"CAMPAIGN_PERFORMANCE_REPORT\",\"DateRangeType\":%s,\"Format\":\"TSV\",\"IncludeVAT\":\"NO\",\"IncludeDiscount\":\"NO\"}}"
  private val ALL_CAMPAIGNS_SELECTION_CRITERIA = "\"DateFrom\":\"%s\",\"DateTo\":\"%s\""
  private val PART_CAMPAIGNS_SELECTION_CRITERIA = "\"DateFrom\":\"%s\",\"DateTo\":\"%s\",\"Filter\":[{\"Field\":\"CampaignId\",\"Operator\":\"IN\",\"Values\":[%s]}]"
  private val SINGLE_CAMPAIGN_SELECTION_CRITERIA = "\"Filter\":[{\"Field\":\"CampaignId\",\"Operator\":\"IN\",\"Values\":[\"%d\"]}]"
}

class Y2GTransfer {
  /**
   * Метод проверяет номера рекламных кампаний на корректность через API Яндекса.<br>
   * Каждый номер кампании проверяется только если ему соответствует ИСТИНА (т.е. в UI указано, что проверка нужна)
   */
  def isCampaignIdValid2(ids: Map[Int, Boolean], token: String, clientLogin: String): Map[Int, Boolean] = {
    var map: Map[Int, Boolean]
    ids.foreach(entry -> {
      map.put(entry.k, )
    })
    Map(1 -> true)
  }

  def isCampaignIdValid(ids: util.Map[Integer, Boolean], token: String, clientLogin: String) =
    ids.entrySet.stream.collect(Collectors.toMap(util.Map.Entry.getKey, (entry: util.Map.Entry[Integer, Boolean]) => entry.getValue && isCampaignIdValid(entry.getKey, token, clientLogin), (k1: Boolean, k2: Boolean) => {
      def foo(k1: Boolean, k2: Boolean) =
        throw Xeption.forDeveloper("При валидации Яндекс найдены дубли номеров кампаний")

      foo(k1, k2)
    }, util.LinkedHashMap.`new`)) //важно отдать пары значений в том же порядке, в котором они были получены

  /**
   * Метод проверяет номер рекламной кампании на корректность через API Яндекса.<br>
   * Если номер корректен, он будет продублирован в ответе.
   * Т.к. в запросе всегда только один номер, для корректного номера последняя строка ответа будет выглядеть "Total rows: 1".<br>
   * Для некорректного номера ответ будет: "Total rows: 0".
   */
  private def isCampaignIdValid(id: Int, token: String, clientLogin: String) = {
    val validRequest = new Nothing("https://api.direct.yandex.com/json/v5/reports") //брать из настроек
    val params = String.format(Y2GTransfer.YANDEX_COMMON_PARAMS, String.format(Y2GTransfer.SINGLE_CAMPAIGN_SELECTION_CRITERIA, id), "\"CampaignId\"", "\"AUTO\"")
    validRequest.setEntity(new Nothing(params, ContentType.APPLICATION_JSON))
    validRequest.setHeader("Accept", "application/json")
    validRequest.setHeader("Authorization", "Bearer " + token)
    validRequest.setHeader("Client-login", clientLogin)
    validRequest.setHeader("Accept-Language", "ru")
    validRequest.setHeader("skipReportHeader", "true") //чтобы в tsv не было первой строки (с заголовком)
    validRequest.setHeader("returnMoneyInMicros", "false")
    var responseEntity = null
    try {
      val httpClient = HttpClients.custom.build
      try {
        val validResponse = httpClient.execute(validRequest)
        if (validResponse.getStatusLine.getStatusCode ne 200) throw Xeption.forDeveloper("ошибка при валидации номера кампании в Яндексе: ответ сервера {0}", validResponse.getStatusLine.getStatusCode)
        responseEntity = EntityUtils.toString(validResponse.getEntity, StandardCharsets.UTF_8)
      } catch {
        case e: IOException =>
          throw Xeption.forDeveloper("ошибка при валидации номера кампании в Яндексе: " + e.getMessage)
      } finally if (httpClient != null) httpClient.close()
    }
    val rows = responseEntity.split("\n")
    // В последней строке (со статистикой) число возвращенных строк кампаний начинается с индекса 12 (типа "Total rows: 1")
    val rowsNumber = rows(rows.length - 1).substring(12)
    rowsNumber.toInt == 1
  }

  //todo add tracer`s using!
  @throws[URISyntaxException]
  @throws[AuthenticationException]
  @throws[IOException]
  @throws[NoSuchAlgorithmException]
  @throws[InvalidKeySpecException]
  def doTransfer(dto: Y2GModel.Dto): String = {
    val log = LoggerFactory.getLogger(classOf[AdsTransferMaintenanceJob])
    val transfer = EntityStorage.get.resolve(dto.ref).getEntity
    val id = "\"" + transfer.getId + "\": "
    var message = id + "произошла непредвиденная ошибка" //постелим соломки на случай, если что пойдет не так
    val downloadRequest = new Nothing(transfer.getReportsUrl)
    val partCampaignsFilterValues = transfer.getCampaigns.stream.filter(Campaign.isActive).filter((c) => c.getValid == null || c.getValid).map((c) => "\"" + c.getId + "\"").collect(Collectors.joining(","))
    //учтем активные кампании - если они есть, работаем с фильтром ТОЛЬКО по ним, если их нет - без фильтра
    val campaignsNumber = transfer.getCampaigns.stream.filter(Campaign.isActive).filter((c) => c.getValid == null || c.getValid).count
    var selectionCriteria = null
    if (campaignsNumber > 0) selectionCriteria = String.format(Y2GTransfer.PART_CAMPAIGNS_SELECTION_CRITERIA, dto.startDate, dto.endDate, partCampaignsFilterValues) //с фильтром по кампаниям
    else selectionCriteria = String.format(Y2GTransfer.ALL_CAMPAIGNS_SELECTION_CRITERIA, dto.startDate, dto.endDate) //без фильтра по кампаниям
    val params = String.format(Y2GTransfer.YANDEX_COMMON_PARAMS, selectionCriteria, "\"Date\",\"Cost\",\"Impressions\",\"Clicks\",\"CampaignName\",\"CampaignId\"", "\"CUSTOM_DATE\"")
    downloadRequest.setEntity(new Nothing(params, ContentType.APPLICATION_JSON))
    downloadRequest.setHeader("Accept", "application/json")
    downloadRequest.setHeader("Authorization", "Bearer " + transfer.getToken)
    downloadRequest.setHeader("Client-login", transfer.getClientLogin)
    downloadRequest.setHeader("Accept-Language", "ru")
    downloadRequest.setHeader("skipReportHeader", "true")
    downloadRequest.setHeader("skipReportSummary", "true") //чтобы в tsv не было последней строки (со статистикой)
    downloadRequest.setHeader("returnMoneyInMicros", "false")
    var yandexData = null
    val pause = 15000
    while ( {
      true
    }) try {
      val httpClient = HttpClients.custom.build
      try {
        val downloadResponse = httpClient.execute(downloadRequest)
        if (downloadResponse.getStatusLine.getStatusCode eq 200) {
          message = id + "отчет Яндекс создан успешно"
          log.info(message)
          yandexData = EntityUtils.toString(downloadResponse.getEntity, StandardCharsets.UTF_8)
          break //todo: break is not supported
        }
        if (downloadResponse.getStatusLine.getStatusCode eq 201) {
          log.info(id + String.format("отчет Яндекс успешно поставлен в очередь в режиме офлайн. Повторная отправка запроса через %d сек.", pause))
          Thread.sleep(pause) //в случае ответов 201 или 202 делаем паузу в 15 сек...
          continue //todo: continue is not supported
          //...и снова пытаемся отправить запрос
        }
        if (downloadResponse.getStatusLine.getStatusCode eq 202) {
          log.info(id + String.format("отчет Яндекс формируется в режиме офлайн. Повторная отправка запроса через %d сек.", pause))
          Thread.sleep(pause) //todo сколько попыток делать?
          continue //todo: continue is not supported
        }
        if (downloadResponse.getStatusLine.getStatusCode eq 400) {
          message = id + "параметры запроса в Яндекс указаны неверно или достигнут лимит отчетов в очереди"
          log.warn(message)
          break //todo: break is not supported
        }
        if (downloadResponse.getStatusLine.getStatusCode eq 500) {
          message = id + "при формировании отчета Яндекс произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее"
          log.error(message)
          break //todo: break is not supported
        }
        if (downloadResponse.getStatusLine.getStatusCode eq 502) {
          message = id + "время формирования отчета Яндекс превысило серверное ограничение.\n\"Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных"
          log.warn(message)
        }
        else {
          log.error(message)
          break //todo: break is not supported
        }
      } catch {
        case e: InterruptedException =>
          log.info(id + "пауза перед очередной отправкой запроса в Яндекс прервана")
      } finally if (httpClient != null) httpClient.close()
    }
    if (yandexData == null) { //null только для ответов 400, 500 и 502
      return message
    }
    //получим данные из ответа Яндекса и преобразуем их
    val rowsFromYandex = yandexData.split("\n")
    val rowsForGoogle = new Array[String](rowsFromYandex.length)
    rowsForGoogle(0) = "ga:date\tga:medium\tga:source\tga:adClicks\tga:adCost\tga:impressions\tga:campaign\tga:adwordsCampaignID"
    //пропускаем первую строку (заголовки)
    for (i <- 1 until rowsFromYandex.length) {
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
      rowsForGoogle(i) = newRow.toString
    }
    val data = transfer.getGoogleApiSettingsFile.getData
    val settingString = new String(data, StandardCharsets.UTF_8)
    val settingJson = new Nothing().fromJson(settingString, classOf[Nothing])
    val clientEmail = settingJson.get("client_email").getAsString
    val tokenUri = settingJson.get("token_uri").getAsString
    val privateKey = settingJson.get("private_key").getAsString.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\n", "").replaceAll("\\s+", "")
    val bytes = TextCodec.BASE64URL.decode(privateKey)
    val keySpec = new PKCS8EncodedKeySpec(bytes)
    val key = KeyFactory.getInstance("RSA").generatePrivate(keySpec)
    //создание подписанного JWT
    val jws = Jwts.builder.setHeaderParam("typ", "JWT").setIssuer(clientEmail).setAudience //iss: Электронный адрес учетной записи службы
    tokenUri.claim //aud: Дескриптор предполагаемой цели утверждения
    ("scope", "https://www.googleapis.com/auth/analytics.edit https://www.googleapis.com/auth/analytics.readonly https://www.googleapis.com/auth/analytics").setIssuedAt //scope: Разделенный пробелами список разрешений, запрашиваемых приложением.
    (new Date).setExpiration //iat: Время, когда было выдано утверждение.
    new Date(System.currentTimeMillis + 3000000).signWith //exp: Время истечения срока действия утверждения - максимум 1 час после выданного времени.
    (SignatureAlgorithm.RS256, key).compact
    //начало авторизации Google
    val tokenPairs = new util.ArrayList[Nothing]
    tokenPairs.add(new Nothing("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"))
    tokenPairs.add(new Nothing("assertion", jws))
    val tokenRequest = new Nothing(tokenUri)
    tokenRequest.setEntity(new Nothing(tokenPairs))
    tokenRequest.setHeader("Content-Type", "application/x-www-form-urlencoded")
    var accessToken = null //код авторизации
    try {
      val httpClient = HttpClients.custom.build
      try {
        val tokenResponse = httpClient.execute(tokenRequest)
        if (tokenResponse.getStatusLine.getStatusCode ne 200) throw Xeption.forDeveloper("ошибка при получении токена аутентификации Google")
        val responseEntity = EntityUtils.toString(tokenResponse.getEntity, StandardCharsets.UTF_8)
        accessToken = new Nothing().fromJson(responseEntity, classOf[Y2GModel.TokenResponse]).accessToken
      } finally if (httpClient != null) httpClient.close()
    }
    log.info(id + "получен токен аутентификации Google")
    //конец авторизации Google
    //подготовим файл для загрузки
    val dataForUpload = new StringBuilder
    util.Arrays.stream(rowsForGoogle).filter(StringUtils.isNotBlank).map((row: String) => row.replace("\t", ",")).forEach((row: String) => dataForUpload.append(row).append("\n"))
    val file = new File("/tmp/from_yandex_for_google.csv")
    try {
      val os = new FileOutputStream(file)
      try os.write(dataForUpload.toString.getBytes(StandardCharsets.UTF_8))
      finally if (os != null) os.close()
    }
    //начало загрузки
    val uploadRequest = new Nothing("https://www.googleapis.com/upload/analytics/v3/management/" + "accounts/" + transfer.getAccountId + "/webproperties/" + transfer.getWebPropertyId + "/customDataSources/" + transfer.getCustomDataSourceId + "/uploads")
    uploadRequest.addHeader("Authorization", "Bearer " + accessToken)
    uploadRequest.addHeader("Content-Type", "application/octet-stream")
    uploadRequest.setEntity(new Nothing(file))
    var uploadTime = null
    try {
      val httpClient = HttpClients.custom.build
      try {
        val uploadResponse = httpClient.execute(uploadRequest)
        if (uploadResponse.getStatusLine.getStatusCode ne 200) throw Xeption.forDeveloper("ошибка при загрузке файла в Google")
        uploadTime = new Nothing().fromJson(EntityUtils.toString(uploadResponse.getEntity, StandardCharsets.UTF_8), classOf[Y2GModel.UploadResponse]).uploadTime
      } finally if (httpClient != null) httpClient.close()
    }
    message = id + " данные переданы в Google " + uploadTime
    log.info(message)
    message
  }
}