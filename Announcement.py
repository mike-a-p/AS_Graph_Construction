class Announcement:
	def __init__(self,prefix,origin,priority,received_from_asn):
		self.prefix = prefix
		self.origin = origin
		self.priority = priority
		self.received_from_asn = received_from_asn
		
		return

	def __str__(self):
		return('Prefix: ' + str(self.prefix) +
			'Origin: ' + str(self.origin) +
			'Priority: ' + str(self.priority) +
			'ASN announcement received from: ' + str(self.as_path)
			)
