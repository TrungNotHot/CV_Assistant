variable "public_key" {
  type = string
  default = "oenflulf"
}

variable "private_key" {
  type = string
  default = "5364fd2f-0c7d-4489-bbb1-1842ab117322"
}

variable "org_id" {
  type = string
  description = "Organiztion ID"
  default = "680fc8399a44ea5814d0cbbb"
}

variable "username" {
  type = string
  description = "username for cluster"
  default = "admin" # you can modify this if needed
}

variable "password" {
  type = string
  description = "pass for cluster"
  default = "admin123" # You can modify this if needed
}
variable "cluster_name" {
  type = string
  description = "Cluster name"
  default = "grab-11-cluster" # Changed from grab_11_cluster to use hyphens instead of underscores
}

variable "cluster_size" {
  type = string
  description = "Cluster size name"
  default = "M0" 
}

variable "region" {
  type = string
  description = "Region name"
  default = "WESTERN_EUROPE" 
}

