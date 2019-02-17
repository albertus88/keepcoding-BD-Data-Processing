package bdproc.common


/*import java.util.Properties
import javax.mail.Message
import javax.mail.MessagingException
import javax.mail.Session
import javax.mail.Transport
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage*/


object EmailUtilities {
  def SendEmail(body: String) = {
   /* // Recipient's email ID needs to be mentioned.
    val to = "abc82@gmail.com";

    // Sender's email ID needs to be mentioned
    val from = "xyz@gmail.com";

    // Assuming you are sending email from localhost
    val host = "localhost";

    // Get system properties
    val properties = System.getProperties();

    // Setup mail server
    properties.setProperty("mail.smtp.host", host);

    // Get the default Session object.
    val session = Session.getDefaultInstance(properties);

    try {
      // Create a default MimeMessage object.
      val message = new MimeMessage(session);

      // Set From: header field of the header.
      message.setFrom(new InternetAddress(from));

      // Set To: header field of the header.
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

      // Set Subject: header field
      message.setSubject("Report outliers prizes");

      // Now set the actual message
      message.setText(body);

      // Send message
      Transport.send(message);
      System.out.println("Sent message successfully....");
    }*/
  }
}
