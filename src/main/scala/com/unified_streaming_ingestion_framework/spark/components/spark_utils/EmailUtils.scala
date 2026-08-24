package com.unified_streaming_ingestion_framework.spark.components.spark_utils

import org.apache.commons.mail.resolver.DataSourceUrlResolver
import org.apache.commons.mail._

import java.net.URL

object EmailUtils {

  /**
   * Send text mail to person
   *
   * @param toAddress   : String
   * @param subject     : String
   * @param bodyMessage : String
   */

  def sendEmail(toAddress: String, subject: String, bodyMessage: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val email = initializeSimpleMail(subject, bodyMessage, EMAIL_HOST_NAME, EMAIL_FROM_ADDRESS)
    email.addTo(toAddress)
    email.send()
  }

  private def initializeSimpleMail(subject: String, bodyMessage: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Email = {

    val email = new SimpleEmail
    email.setHostName(EMAIL_HOST_NAME)
    email.setFrom(EMAIL_FROM_ADDRESS)
    email.setSmtpPort(25)
    email.setStartTLSEnabled(false)
    //email.setAuthentication("rajesh.m@sh.com", "P@ssword")
    email.setSubject(subject)
    email.setMsg(bodyMessage)
    email

  }

  /**
   * Send text mail to multiple persons
   *
   * @param toAddress   : List(String), list of mail id's
   * @param subject     : String
   * @param bodyMessage : String
   */

  def sendEmail(toAddress: List[String], subject: String, bodyMessage: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val email = initializeSimpleMail(subject, bodyMessage, EMAIL_HOST_NAME, EMAIL_FROM_ADDRESS)
    email.addTo(toAddress: _*)
    email.send()
  }

  /**
   * Used to send simple mail to only one person
   *
   * @param toAddress   : String, To Address
   * @param subject     : String, subject of the email
   * @param bodyMessage : String, Body of the email
   * @param AttachementPath
   */

  def sendEmailWithAttachments(toAddress: String, subject: String, bodyMessage: String, AttachmentPath: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val email = multiPartEmailInitialize(subject, bodyMessage, EMAIL_HOST_NAME, EMAIL_FROM_ADDRESS)
    val attachment = attachmentInitialize(attachmentPath = AttachmentPath)
    email.addTo(toAddress)
    email.attach(attachment)
    email.send()
  }

  private def multiPartEmailInitialize(subject: String, bodyMessage: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): MultiPartEmail = {
    val email = new MultiPartEmail
    email.setHostName(EMAIL_HOST_NAME)
    email.setFrom(EMAIL_FROM_ADDRESS)
    email.setSubject(subject)
    email.setMsg(bodyMessage)
    email
  }

  private def attachmentInitialize(attachmentPath: String, description: Option[String] = None, name: Option[String] = None): EmailAttachment = {
    val attachment: EmailAttachment = new EmailAttachment()
    attachment.setPath(attachmentPath)
    attachment.setDisposition(EmailAttachment.ATTACHMENT)
    if (description.isDefined) attachment.setPath(description.get)
    if (name.isDefined) attachment.setName(name.get)
    attachment
  }

  def sendHtmlTableMail(toAddress: String, subject: String, bodyMessage: String, html: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val email = new ImageHtmlEmail()
    email.setHostName(EMAIL_HOST_NAME)
    email.setFrom(EMAIL_FROM_ADDRESS)
    email.addTo(toAddress)
    email.setSubject(subject)
    email.setMsg(bodyMessage)
    email.setHtmlMsg(html)
    email.send()
  }

  def sendHtmlMail(toAddress: String, subject: String, bodyMessage: String, imagePathList: List[String], html: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val email = new ImageHtmlEmail()
    email.setHostName(EMAIL_HOST_NAME)
    email.setFrom(EMAIL_FROM_ADDRESS)
    email.addTo(toAddress)
    email.setSubject(subject)
    email.setMsg(bodyMessage)
    for (imagePath <- imagePathList) {
      val url = new URL(s"file://localhost/$imagePath")
      email.setDataSourceResolver(new DataSourceUrlResolver(url))
    }
    email.setHtmlMsg(html)
    email.send()
  }

  def sendHtmlErrorMail(toAddress: String, subject: String, bodyMessage: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val html =
      s"""
         |<!DOCTYPE html>
         |<html lang="en">
         |<head>
         |    <meta charset="UTF-8">
         |    <meta name="viewport"
         |          content="width=device-width,
         |          initial-scale=1.0">
         |    <title>Error Log</title>
         |    <style>
         |        body{
         |        font-family: Arial, sans-serif;
         |        margin: 0;
         |        padding: 20px;
         |        background-color: #f5f5f5;
         |        }
         |        .container{
         |            max-width: 600px;
         |            margin: 0 auto;
         |            background-color: #fff;
         |            padding: 2-px;
         |            border-radius: 5px;
         |            box-shadow: 0px 2px 5 px
         |rgba(0,0,0,0.1);
         |        }
         |        h1{
         |           color:#333;
         |        }
         |        p{
         |           color:#666;
         |        }
         |    </style>
         |</head>
         |<body>
         |    <div class="container">
         |        <h1>Error Log</h1>
         |        <p>An error has occurred on the pipeline, please find the details below:</p>
         |
         |        <h2>Error Details:</h2>
         |        <ul>
         |            <li><strong>Error Message:</strong></li>
         |            <li><strong>$bodyMessage</strong></li>
         |        </ul>
         |    </div>
         |
         |</body>
         |</html>
         |""".stripMargin
    val email = new ImageHtmlEmail()
    email.setHostName(EMAIL_HOST_NAME)
    email.setFrom(EMAIL_FROM_ADDRESS)
    email.addTo(toAddress.split(","): _*)
    email.setSubject(subject)
    email.setHtmlMsg(html)
    email.send()
  }

  def sendHtmlSuccessMail(toAddress: String, subject: String, bodyMessage: String, EMAIL_HOST_NAME: String, EMAIL_FROM_ADDRESS: String): Unit = {
    val html =
      s"""
         |<!DOCTYPE html>
         |<html lang="en">
         |<head>
         |    <meta charset="UTF-8">
         |    <meta name="viewport"
         |          content="width=device-width,
         |          initial-scale=1.0">
         |    <title>Job Success</title>
         |    <style>
         |        body{
         |        font-family: Arial, sans-serif;
         |        margin: 0;
         |        padding: 20px;
         |        background-color: #f5f5f5;
         |        }
         |        .container{
         |            max-width: 600px;
         |            margin: 0 auto;
         |            background-color: #fff;
         |            padding: 2-px;
         |            border-radius: 5px;
         |            box-shadow: 0px 2px 5 px
         |rgba(0,0,0,0.1);
         |        }
         |        h1{
         |           color:#333;
         |        }
         |        p{
         |           color:#666;
         |        }
         |    </style>
         |</head>
         |<body>
         |    <div class="container">
         |        <h1>Job Successfully Completed</h1>
         |        <p>The pipeline executed successfully, please find the details below:</p>
         |
         |        <h2>Job Details:</h2>
         |        <ul>
         |            <li><strong>$bodyMessage</strong></li>
         |        </ul>
         |    </div>
         |
         |</body>
         |</html>
         |""".stripMargin
    val email = new ImageHtmlEmail()
    email.setHostName(EMAIL_HOST_NAME)
    email.setFrom(EMAIL_FROM_ADDRESS)
    email.addTo(toAddress.split(","): _*)
    email.setSubject(subject)
    email.setHtmlMsg(html)
    email.send()
  }

}
