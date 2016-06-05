package org.qbroker.net;

/* MessageMailer.java - A generic mailer to send emails */

import java.util.Properties;
import java.util.Date;
import java.io.FileInputStream;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.AddressException;
import org.qbroker.common.TraceStackThread;

/**
 * MessageMailer is a generic mailer to send emails
 *<br/><br/>
 * It takes an email address of recipient, your return address, and the subject
 * of the mail as the argument at the initial stage.  The send() method takes a
 * String of the message body as the argument and calls JavaMail API to mail the
 * message.  It does not guarantee the mail being delivered.
 *<br/>
 * @author yannanlu@yahoo.com
 */

public class MessageMailer {
    public static String smtpHost = null;
    private InternetAddress toAddress = null;
    private InternetAddress fromAddress = null;
    private Session session = null;
    private Transport transport = null;
    private MimeMessage msg = null;
    private String subject = null;

    public MessageMailer(String to, String from, String subject) {
        Properties props = new Properties();
        if (smtpHost != null && smtpHost.length() > 0)
            props.setProperty("mail.smtp.host", smtpHost);
        if (to != null && to.length() > 0)
            toAddress = getMailAddress(to);

        if (from != null && from.length() > 0) {
            fromAddress = getMailAddress(from);
            props.setProperty("mail.smtp.from", from);
        }
        else {
            String owner = System.getProperty("user.name");
            String hostName = null;
            try {
                hostName =
                    java.net.InetAddress.getLocalHost().getCanonicalHostName();
            }
            catch (java.net.UnknownHostException e) {
            }
            if (hostName != null && hostName.length() > 0)
                owner += "@" + hostName;
            fromAddress = getMailAddress(owner);
            props.setProperty("mail.smtp.from", owner);
        }

        if (subject != null)
            this.subject = subject;

        session = Session.getInstance(props, null);
        try {
            transport = session.getTransport("smtp");
        }
        catch (NoSuchProviderException e) {
            throw new IllegalArgumentException("no smtp provider");
        }
        msg = new MimeMessage(session);
    }

    public void send(String text) throws MessagingException {
        send(null, text);
    }

    public synchronized void send(String subject, String text)
        throws MessagingException {
        msg.setSentDate(new Date());
        msg.setFrom(fromAddress);
        if (subject != null)
            msg.setSubject(subject);
        else if (this.subject != null)
            msg.setSubject(this.subject);
        msg.setRecipient(Message.RecipientType.TO, toAddress);
        msg.setText(text);
        Transport.send(msg);
    }

    public void send(String subject, String text, String [] recipients)
        throws MessagingException {
        InternetAddress [] addresses = new InternetAddress[recipients.length];
        for (int i=0; i<recipients.length; i++) {
            addresses[i] = getMailAddress(recipients[i]);
        }
        send(subject, text, addresses);
    }

    public void send(String text, InternetAddress [] recipients)
        throws MessagingException {
        send(null, text, recipients);
    }

    public synchronized void send(String subject, String text,
        InternetAddress [] recipients) throws MessagingException {
        if (recipients.length == 0) {
            send(subject, text);
            return;
        }

        msg.setSentDate(new Date());
        msg.setFrom(fromAddress);
        msg.setRecipients(Message.RecipientType.TO, recipients);
        if (subject != null)
            msg.setSubject(subject);
        else if (this.subject != null)
            msg.setSubject(this.subject);

        msg.setText(text);
        Transport.send(msg);
    }

    public static InternetAddress getMailAddress(String address) {
        if (address == null || address.length() == 0)
            throw new IllegalArgumentException("empty mail address");
        InternetAddress intAddress;
        try {
            intAddress = new InternetAddress(address);
        }
        catch (AddressException e) {
            throw new IllegalArgumentException(
                "failed to get mail address out of: " + address + " - " +
                TraceStackThread.traceStack(e));
        }

        return intAddress;
    }

    /** this is used to reset the default subject only, not MT-Safe */
    public void setSubject(String subject) {
       this.subject = subject;
    }

    public static void setSMTPHost(String mailHost) {
       if (smtpHost == null && mailHost != null && mailHost.length() > 0)
           smtpHost = mailHost;
    }

    public static void main(String args[]) {
        MessageMailer mailer = null;
        Properties props = new Properties();
        try {
            props = new Properties();
            FileInputStream in = new FileInputStream("MailTest.properties");
            props.load(in);

            if (props.getProperty("mail.smtp.host") != null) {
                MessageMailer.setSMTPHost(props.getProperty("mail.smtp.host"));
            }
            mailer = new MessageMailer(props.getProperty("ToAddress"),
                props.getProperty("FromAddress"),
                props.getProperty("Subject"));
            mailer.send(props.getProperty("Text"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
