#Author: Jonathan Chen
#Due Date: N/A
#Assignment: This is the main file that for the Final Project for MIS380P.
#           The goal is a program to store and manage existing inventory, and allow customers to purchase or
#           return bakery supplies. The shop has provided a text file containing existing inventory data for you.
#           You will use classes to manage this data. 

import transactionitem
import inventory

#process the inventory file
def process_inventoryFile(filename):
    #create a list to store inventory file
    inventory_list = []
    # open the inventory.txt file
    inventory_file = open(filename, 'r')

    while True:
        #read the first record's name for each inventory item
        item_id = inventory_file.readline().strip()
        if not item_id:  # If item_id is empty, it means end of file
                break

        #read the item name
        item_name = str(inventory_file.readline().strip())

        #read the item stock quantity
        item_stock = int(inventory_file.readline().strip())
        
        #read the item price
        item_price = float(inventory_file.readline().strip())

        #create an instance of the inventory class for the items
        item = inventory.inventory(item_id, item_name, item_stock, item_price)

        #add the items to the inventory_list
        inventory_list.append(item)

    return inventory_list

    #close the file
    inventory_file.close()
        
#print the inventory menu
def print_inventoryMenu(inventory_list):
    print("ID\tItem\t\t\t\tPrice\t\tQty Available")
    print("-" * 70)
    #read each "item" from inventory list
    for item in inventory_list:
        print(item)

#updates the new inventory file
def update_inventoryFile(inventory_list, filename):
    #open the file
    inventory_file = open(filename, 'w')
    for item in inventory_list:
        #writes contents into a new updated file
        inventory_file.write(f"{item.get_id()}\n")
        inventory_file.write(f"{item.get_name()}\n")
        inventory_file.write(f"{item.get_stock()}\n")
        inventory_file.write(f"{item.get_price()}\n")
    #closes the inventory file
    inventory_file.close()

#get the item id from the user
def input_itemID(inventory_list):
    while True:
        
        #Ask user for product ID
        Product_ID = input("Which product ID would you like to purchase? Enter 0 to exit. ")
        #if user enters "0"
        
        if Product_ID == '0':
            return None
        found = False
        
        #read each "item" from inventory list
        for item in inventory_list:
            #use the get method to get the ID and see if it matches Product ID
            if item.get_id() == Product_ID:
                found = True
                break
        
        if found:
            return Product_ID
        else:
            print("Invalid Product ID. Please try again.")

#print the invoice of the transactions
def print_invoice(transactions):

    #initialized the variables that will be printed
    total_cost = 0
    total_qty = 0
    print("\n")
    print("Order Complete. See Invoice Below:")
    print("ID\tItem\t\t\tQuantity\tPrice\t  Extended Cost")
    print("-" * 70)
    #This uses the get method from the transactionItem
    for trans in transactions:
        item_id = trans.get_id()
        item_name = trans.get_name()
        item_qty = trans.get_qty()
        item_price = trans.get_price()
        item_total_cost = trans.calc_cost()
        total_cost += item_total_cost
        total_qty += int(item_qty)
        max_width = 8
        print(trans)
    tax = total_cost * 0.085
    total_amount_due = total_cost + tax
    print("-" * 70)
    print(f"Total Items: {total_qty}")
    print(f"Subtotal: ${total_cost:,.2f}")
    print(f"Tax (8.5%): ${tax:,.2f}")
    print(f"Total Amount Due: ${total_amount_due:,.2f}")   

def main():
    inventory_list = process_inventoryFile('inventory.txt')

    transactions = []

    while True:
        #uses the "def print_inventoryMenu"
        print_inventoryMenu(inventory_list)
        #uses the "def input_itemID"
        Product_ID = input_itemID(inventory_list)

        #This is when the user enters "0" to be done and thereby break out of the while loop
        if Product_ID is None:
            break

        #This initilizes the variable to store the selected inventory item
        item = None
        #This goes through the list of inventory list to see if the inventory list has the "product ID"
        for item in inventory_list:
            if item.get_id() == Product_ID:
                break

        try:
            product_qty = int(input("How many items would you like to purchase? Enter a negative number for a return: "))
            #This looks if the quantity is equal to 0
            if product_qty == 0:
                print("The quantity must be greater than or less than zero")
                continue

            #This looks if the quantity is greater than 0
            if product_qty > 0:
                
                #If it is this tries to "purchases" the item based on the quantity user inputed
                if item.purchase(abs(product_qty)):
                    
                    #this creates the transactionItem object and adds it to the transaction list
                    trans_item = transactionitem.TransactionItem()
                    trans_item.set_id(item.get_id())
                    trans_item.set_name(item.get_name())
                    trans_item.set_qty(product_qty)
                    trans_item.set_price(item.get_price())
                    transactions.append(trans_item)
                    
                #If there is not enough inventory this will inform the user
                else:
                    print("There is not enough inventory to purchase this many of the item. Try again.")
                    continue

            #This is if the quantity the user inputed is less than 0 or a negative number
            else:
                #this tries to "restock" the item based on the quantity the user inputed
                if item.restock(abs(product_qty)):  #Absolute value for restock
                    #this creates the transaction Item object with a negative quantity and add it to the transaction list
                    trans_item = transactionitem.TransactionItem()
                    trans_item.set_id(item.get_id())
                    trans_item.set_name(item.get_name())
                    trans_item.set_qty(product_qty)  #Negative quantity for return
                    trans_item.set_price(item.get_price())
                    transactions.append(trans_item)
                else:
                    continue



        except ValueError:
            #This is for when the user inputs a invalid quantity
            print("There is not enough inventory to purchase this many of the item. Try again.")
            continue

    #This prints if no transaction were made
    if not transactions:
        print("Thank you for visiting.")
    #This writes to a updated inventory file named "UpdatedInventory.txt"
    update_inventoryFile(inventory_list, 'UpdatedInventory.txt')

    #If transactions were made the invoice will print
    if transactions:
        print_invoice(transactions)

main()



 
