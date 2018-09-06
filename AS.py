#from Ordered_Set import Ordered_Set

class AS:
	def __init__(self,asn):
		self.asn = asn
		self.rank = None
		self.providers = list()
		self.temp_neighbors = None
		self.peers = list()
		self.customers = list()
		self.anns_from_customers = list()
		self.anns_from_peers_providers = list()
		self.anns_sent_to_peers_providers = list()
		#variables for Tarjan's Alg
		self.index = None
		self.lowlink = None
		self.onstack = False
		self.SCC_id = None
	
	def __str__(self):
		return ('ASN: ' + str(self.asn) + ' Rank: ' + str(self.rank)
			+ ' Providers: ' + str(self.providers)
			+ ' Peers: ' + str(self.peers)
			+ ' Customerss: ' + str(self.customers)
			+ ' Index ' + str(self.index)
			+ ' Lowlink ' + str(self.lowlink)
			+ ' Onstack ' + str(self.onstack)
			+ ' SCC_id ' + str(self.SCC_id))

	def add_neighbor(self,asn,relationship):
		if(relationship == 0):	
			self.append_no_dup(self.providers,asn)
		if(relationship == 1):	
			self.append_no_dup(self.peers,asn)
		if(relationship == 2):	
			self.append_no_dup(self.customers,asn)
	
	def update_rank(self,rank):
		#updates rank if new rank is higher
		#returns 1 if changed, 0 if not
		old = self.rank
		self.rank = max(self.rank, rank)
		if(old!=self.rank):
			return 1
		else:
			return 0

	def all_announcements(self):
		return (self.providers +
			self.peers +
			self.customers)

	def give_announcement(self,announcement):
		if(announcement.received_from == 0 or 
			announcement.received_from == 1):
			self.anns_from_peers_providers.append(announcement)
		else:
			self.anns_from_customers.append(announcement)

	def append_no_dup(self, this_list, asn, l = None, r = None):

		#initialize l and r on first call
		if(l is None and r is None):
			l = 0
			r = len(this_list)-1
		#if r is greater than l, continue binary search
		if(r>=l):
			half = int(l + (r-l)/2)
			#if asn is found in the list, return without inserting
			if(this_list[half] == asn):
				return
			elif(this_list[half] > asn):
				return self.append_no_dup(this_list, asn, l, half-1)
			else:
				return self.append_no_dup(this_list, asn, half+1, r)
		#if r is less than l, insert asn
		else:
			this_list[r+1:r+1] = [asn]
		return

