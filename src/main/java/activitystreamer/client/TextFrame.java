package activitystreamer.client;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.border.Border;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import activitystreamer.util.Settings;
import activitystreamer.util.Strings;


@SuppressWarnings("serial")
public class TextFrame extends JFrame implements ActionListener {
	private static final Logger log = LogManager.getLogger();
	private JTextArea inputText;
	private JTextArea outputText;
	private JButton sendButton;
	private JButton disconnectButton;
	private JSONParser parser = new JSONParser();
	
	public TextFrame(){
		setTitle("Chat");
		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new GridLayout(1,2));
		JPanel inputPanel = new JPanel();
		JPanel outputPanel = new JPanel();
		inputPanel.setLayout(new BorderLayout());
		outputPanel.setLayout(new BorderLayout());
		Border lineBorder = BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.lightGray),"Chat Input");
		inputPanel.setBorder(lineBorder);
		lineBorder = BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.lightGray),"Chat Output");
		outputPanel.setBorder(lineBorder);
		outputPanel.setName("Text output");
		
		inputText = new JTextArea();
		JScrollPane scrollPane = new JScrollPane(inputText);
		inputPanel.add(scrollPane,BorderLayout.CENTER);
		
		JPanel buttonGroup = new JPanel();
		sendButton = new JButton("Send");
		disconnectButton = new JButton("Disconnect");
		buttonGroup.add(sendButton);
		buttonGroup.add(disconnectButton);
		inputPanel.add(buttonGroup,BorderLayout.SOUTH);
		sendButton.addActionListener(this);
		disconnectButton.addActionListener(this);
		
		
		outputText = new JTextArea();
		scrollPane = new JScrollPane(outputText);
		outputPanel.add(scrollPane,BorderLayout.CENTER);
		
		mainPanel.add(inputPanel);
		mainPanel.add(outputPanel);
		add(mainPanel);
		
		setLocationRelativeTo(null); 
		setSize(1280,768);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setVisible(true);
	}

	public void setOutputText(String text){
		outputText.setText(text);
		outputText.revalidate();
		outputText.repaint();
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		if(e.getSource()==sendButton){
			String msg = inputText.getText().trim().replaceAll("\r","").replaceAll("\n","").replaceAll("\t", "");
			JSONObject obj = new JSONObject();
			obj.put(Strings.MESSAGE, msg);
			ClientSkeleton.getInstance().sendJsonOnSocket(obj);
			
		} else if(e.getSource()==disconnectButton){
			ClientSkeleton.getInstance().disconnect();
		}
	}
}
