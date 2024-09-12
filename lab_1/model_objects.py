from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List, Optional
from enum import Enum


class CustomerType(Enum):
    """type of customer

    Args:
        Enum (enum): person or company
    """    
    PERSON = 1
    COMPANY = 2

@dataclass_json
@dataclass
class ShoppingList:
    """
    shopping list
    """    
    products: List[str] = field(default_factory=list)

@dataclass_json
@dataclass(frozen=True)
class Address:
    """
    adress of customer
    """    
    street: str
    city: str
    postalCode: str

@dataclass_json
@dataclass(frozen=True)
class ExternalCustomer:
    """
    external customer
    """    
    externalId: str
    name: str
    isCompany: bool
    companyNumber: Optional[str]
    preferredStore: str
    postalAddress: Address
    shoppingLists: List[ShoppingList] = field(default_factory=list)


class Customer:
    """
    customer info
    """
    def __init__(self, internalId: str = None, 
                 externalId: str = None, 
                 masterExternalId: str = None, 
                 name: str = None,
                 customerType: CustomerType = None, 
                 companyNumber: str = None):
        self.internalId = internalId
        self.externalId = externalId
        self.masterExternalId = masterExternalId
        self.name = name
        self.customerType = customerType
        self.companyNumber = companyNumber
        self.shoppingLists = []
        self.address = None

    def addShoppingList(self, shoppingList: ShoppingList) -> None:
        """add to shopping list

        Args:
            shoppingList (ShoppingList): shopping list 
        """        
        self.shoppingLists.append(shoppingList)
