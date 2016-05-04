package DynamoDB;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class CreateTable {
	public static void create(String name, DynamoDB client) throws InterruptedException{
		
		ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName("Name").withAttributeType("S"));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName("Content").withAttributeType("S"));
		
		ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
		keySchema.add(new KeySchemaElement().withAttributeName("Name").withKeyType(KeyType.HASH));
		
		CreateTableRequest request = new CreateTableRequest()
											.withTableName(name)
											.withKeySchema(keySchema)
											.withAttributeDefinitions(attributeDefinitions)
											.withProvisionedThroughput(new ProvisionedThroughput()
										    .withReadCapacityUnits(1L)
											.withWriteCapacityUnits(1L));
		Table table = client.createTable(request);
		table.waitForActive();
	}
}
